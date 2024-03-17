// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::coord::{Coordinator, Message};
use itertools::Itertools;
use mz_catalog::memory::objects::{CatalogItem, ClusterVariant, ClusterVariantManaged};
use mz_controller_types::ClusterId;
use mz_ore::soft_panic_or_log;
use mz_sql::catalog::CatalogCluster;
use mz_sql_parser::ast::ClusterScheduleOptionValue;
use tracing::warn;

const POLICIES: &[&str] = &[REFRESH_POLICY_NAME];

const REFRESH_POLICY_NAME: &str = "refresh";

impl Coordinator {
    #[mz_ore::instrument(level = "debug")]
    /// Call each scheduling policy, gather their decisions about turning clusters on/off, and send
    /// a message with the result.
    pub(crate) async fn check_scheduling_policies(&mut self) {
        // (So far, we have only this one policy.)
        let refresh_policy_decisions = self.check_refresh_policy().await;

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        if let Err(e) = internal_cmd_tx.send(Message::SchedulingDecisions(vec![(
            REFRESH_POLICY_NAME,
            refresh_policy_decisions,
        )])) {
            // It is not an error for this task to be running after `internal_cmd_rx` is dropped.
            warn!("internal_cmd_rx dropped before we could send: {:?}", e);
        }
    }

    async fn check_refresh_policy(&mut self) -> Vec<(ClusterId, bool)> {
        let mut decisions: Vec<(ClusterId, bool)> = Vec::new();
        for cluster in self.catalog().clusters() {
            if let ClusterVariant::Managed(ref config) = cluster.config.variant {
                match config.schedule {
                    ClusterScheduleOptionValue::Manual => {
                        // nothing to do, user manages it manually
                    }
                    ClusterScheduleOptionValue::Refresh => {
                        let refresh_mvs = cluster
                            .bound_objects()
                            .iter()
                            .filter_map(|id| {
                                if let CatalogItem::MaterializedView(mv) =
                                    self.catalog().get_entry(id).item()
                                {
                                    if mv.refresh_schedule.is_some() {
                                        Some(id)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect_vec();

                        // Check whether there is at least one REFRESH MV needing a refresh.
                        let local_read_ts = self.get_local_read_ts().await;
                        decisions.push((
                            cluster.id,
                            refresh_mvs.into_iter().any(|mv| {
                                let mv_frontier = &self
                                    .controller
                                    .storage
                                    .collection(*mv)
                                    .expect("the storage controller should know about MVs that exist in the catalog")
                                    .write_frontier;
                                mv_frontier.less_than(&local_read_ts)
                            }),
                        ));
                    }
                }
            }
        }
        decisions
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn handle_scheduling_decisions(
        &mut self,
        decisions: Vec<(&'static str, Vec<(ClusterId, bool)>)>,
    ) {
        for (policy_name, decisions) in decisions.iter() {
            for (cluster_id, decision) in decisions {
                self.scheduling_decisions
                    .entry(*cluster_id)
                    .or_insert_with(Default::default)
                    .insert(policy_name, *decision);
            }
        }

        // Clean up those clusters from `scheduling_decisions` that
        // - have been dropped, or
        // - were switched to unmanaged, or
        // - were switched to `SCHEDULE = MANUAL`.
        for cluster_id in self.scheduling_decisions.keys().cloned().collect_vec() {
            match self.get_managed_cluster_config(cluster_id) {
                None => {
                    // Cluster have been dropped or switched to unmanaged.
                    self.scheduling_decisions.remove(&cluster_id);
                }
                Some(managed_config) => {
                    if matches!(managed_config.schedule, ClusterScheduleOptionValue::Manual) {
                        self.scheduling_decisions.remove(&cluster_id);
                    }
                }
            }
        }

        for (cluster_id, decisions) in self.scheduling_decisions.clone() {
            // If all policies have made a decision about this cluster
            if POLICIES.iter().all(|policy| decisions.contains_key(policy)) {
                // check whether the cluster's state matches the needed state.
                let needs_replica = decisions.values().contains(&true);
                let cluster_config = self
                    .get_managed_cluster_config(cluster_id)
                    .expect("cleaned up non-existing and unmanaged clusters above");
                let has_replica = cluster_config.replication_factor > 0; // Is it on?
                if needs_replica != has_replica {
                    // Turn the cluster on or off.
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
