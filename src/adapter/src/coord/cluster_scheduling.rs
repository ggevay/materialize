// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::coord::{Coordinator, Message};
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_catalog::memory::objects::{CatalogItem, ClusterVariant, ClusterVariantManaged};
use mz_controller_types::ClusterId;
use mz_ore::soft_panic_or_log;
use mz_sql::catalog::CatalogCluster;
use mz_sql_parser::ast::ClusterScheduleOptionValue;
use timely::progress::Antichain;
use tracing::warn;

const POLICIES: &[&str] = &[REFRESH_POLICY_NAME];

const REFRESH_POLICY_NAME: &str = "refresh";

impl Coordinator {
    #[mz_ore::instrument(level = "debug")]
    /// Call each scheduling policy.
    pub(crate) async fn check_scheduling_policies(&mut self) {
        // (So far, we have only this one policy.)
        self.check_refresh_policy();
    }

    /// Runs the `SCHEDULE = ON REFRESH` cluster scheduling policy, which makes cluster On/Off
    /// decisions based on REFRESH materialized view write frontiers and the current time (the local
    /// oracle read ts), and sends `Message::SchedulingDecisions` with these decisions.
    /// (Queries the timestamp oracle on a background task.)
    fn check_refresh_policy(&mut self) {
        // Collect the smallest REFRESH MV write frontiers per cluster.
        let mut min_refresh_mv_write_frontiers = Vec::new();
        for cluster in self.catalog().clusters() {
            if let ClusterVariant::Managed(ref config) = cluster.config.variant {
                match config.schedule {
                    ClusterScheduleOptionValue::Manual => {
                        // Nothing to do, user manages this cluster manually.
                    }
                    ClusterScheduleOptionValue::Refresh => {
                        min_refresh_mv_write_frontiers.push((
                            cluster.id,
                            cluster
                            .bound_objects()
                            .iter()
                            .filter_map(|id| {
                                if let CatalogItem::MaterializedView(mv) =
                                    self.catalog().get_entry(id).item()
                                {
                                    if mv.refresh_schedule.is_some() {
                                        Some(&self
                                            .controller
                                            .storage
                                            .collection(*id)
                                            .expect("the storage controller should know about MVs that exist in the catalog")
                                            .write_frontier)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .fold(Antichain::new(), |ac1, ac2| Lattice::meet(&ac1, ac2)))
                        );
                    }
                }
            }
        }

        // Spawn a background task that queries the timestamp oracle for the current read timestamp,
        // compares this ts with the REFRESH MV write frontiers, thus making On/Off decisions per
        // cluster, and sends a `Message::SchedulingDecisions` with these decisions.
        let ts_oracle = self.get_local_timestamp_oracle();
        let tokio_handle = tokio::runtime::Handle::current();
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        mz_ore::task::spawn_blocking(
            || "refresh policy get ts and make decisions",
            move || {
                let local_read_ts = tokio_handle.block_on(async { ts_oracle.read_ts().await });

                let decisions = min_refresh_mv_write_frontiers
                    .into_iter()
                    .map(|(cluster_id, min_refresh_mv_write_frontier)| {
                        (
                            cluster_id,
                            min_refresh_mv_write_frontier.less_than(&local_read_ts),
                        )
                    })
                    .collect();

                if let Err(e) = internal_cmd_tx.send(Message::SchedulingDecisions(vec![(
                    REFRESH_POLICY_NAME,
                    decisions,
                )])) {
                    // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
                    warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                }
            },
        );
    }

    /// Handles `SchedulingDecisions`:
    /// 1. Adds the newly made decisions to `cluster_scheduling_decisions`.
    /// 2. Cleans up old decisions that are for clusters no longer in scope of automated scheduling
    ///   decisions.
    /// 3. For each cluster, it sums up `cluster_scheduling_decisions`, checks the summed up decision
    ///   against the cluster state, and turns cluster On/Off if needed.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn handle_scheduling_decisions(
        &mut self,
        decisions: Vec<(&'static str, Vec<(ClusterId, bool)>)>,
    ) {
        // 1. Add the received decisions to `cluster_scheduling_decisions`.
        for (policy_name, decisions) in decisions.iter() {
            for (cluster_id, decision) in decisions {
                self.cluster_scheduling_decisions
                    .entry(*cluster_id)
                    .or_insert_with(Default::default)
                    .insert(policy_name, *decision);
            }
        }

        // 2. Clean up those clusters from `scheduling_decisions` that
        // - have been dropped, or
        // - were switched to unmanaged, or
        // - were switched to `SCHEDULE = MANUAL`.
        for cluster_id in self
            .cluster_scheduling_decisions
            .keys()
            .cloned()
            .collect_vec()
        {
            match self.get_managed_cluster_config(cluster_id) {
                None => {
                    // Cluster have been dropped or switched to unmanaged.
                    self.cluster_scheduling_decisions.remove(&cluster_id);
                }
                Some(managed_config) => {
                    if matches!(managed_config.schedule, ClusterScheduleOptionValue::Manual) {
                        self.cluster_scheduling_decisions.remove(&cluster_id);
                    }
                }
            }
        }

        // 3. Act on `scheduling_decisions` where needed.
        for (cluster_id, decisions) in self.cluster_scheduling_decisions.clone() {
            // If all policies have made a decision about this cluster
            if POLICIES.iter().all(|policy| decisions.contains_key(policy)) {
                // check whether the cluster's state matches the needed state.
                let needs_replica = decisions.values().contains(&true);
                let cluster_config = self
                    .get_managed_cluster_config(cluster_id)
                    .expect("cleaned up non-existing and unmanaged clusters above");
                let has_replica = cluster_config.replication_factor > 0; // Is it On?
                if needs_replica != has_replica {
                    // Turn the cluster On or Off.
                    let mut new_config = cluster_config.clone();
                    new_config.replication_factor = if needs_replica { 1 } else { 0 };
                    if let Err(e) = self
                        .sequence_alter_cluster_managed_to_managed(
                            None,
                            cluster_id,
                            &cluster_config,
                            new_config.clone(),
                        )
                        .await
                    {
                        soft_panic_or_log!(
                            "handle_scheduling_decisions couldn't alter cluster {}. \
                             Old config: {:?}, \
                             New config: {:?}, \
                             Error: {}",
                            cluster_id,
                            cluster_config,
                            new_config,
                            e
                        );
                    }
                }
            }
        }
    }

    /// Returns the managed config for a cluster. Returns None if the cluster doesn't exist or if
    /// it's an unmanaged cluster.
    fn get_managed_cluster_config(&self, cluster_id: ClusterId) -> Option<ClusterVariantManaged> {
        let cluster = self.catalog().try_get_cluster(cluster_id)?;
        if let ClusterVariant::Managed(managed_config) = cluster.config.variant.clone() {
            Some(managed_config)
        } else {
            None
        }
    }
}
