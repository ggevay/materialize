// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v52 as v52, objects_v53 as v53};
use mz_stash::upgrade::WireCompatible;
use mz_stash::wire_compatible;

wire_compatible!(v52::ClusterKey with v53::ClusterKey);
wire_compatible!(v52::RoleId with v53::RoleId);
wire_compatible!(v52::MzAclItem with v53::MzAclItem);
wire_compatible!(v52::ReplicaLogging with v53::ReplicaLogging);
wire_compatible!(v52::ReplicaMergeEffort with v53::ReplicaMergeEffort);
wire_compatible!(v52::OptimizerFeatureOverride with v53::OptimizerFeatureOverride);

// In v53, we add the `schedule` field to `ClusterConfig`. The migration just needs to make the
// default value of the `schedule` field appear.
pub fn upgrade(
    snapshot: Vec<v52::StateUpdateKind>,
) -> Vec<MigrationAction<v52::StateUpdateKind, v53::StateUpdateKind>> {
    // snapshot
    //     .into_iter()
    //     .filter_map(|update| {
    //         let v52::state_update_kind::Kind::Cluster(
    //             v52::state_update_kind::Cluster { key, value },
    //         ) = update.kind.as_ref().expect("missing field")
    //         else {
    //             return None;
    //         };
    //
    //         let current = v52::StateUpdateKind {
    //             kind: Some(
    //                 v52::state_update_kind::Kind::Cluster(
    //                     v52::state_update_kind::Cluster {
    //                         key: key.clone(),
    //                         value: value.clone(),
    //                     },
    //                 ),
    //             ),
    //         };
    //
    //         let new = v53::StateUpdateKind {
    //             kind: Some(
    //                 v53::state_update_kind::Kind::Cluster(
    //                     v53::state_update_kind::Cluster {
    //                         key: key.as_ref().map(WireCompatible::convert),
    //                         value: value.as_ref().map(|old_val| v53::ClusterValue {
    //                             name: old_val.name.clone(),
    //                             owner_id: old_val.owner_id.as_ref().map(WireCompatible::convert),
    //                             privileges: old_val
    //                                 .privileges
    //                                 .iter()
    //                                 .map(WireCompatible::convert)
    //                                 .collect(),
    //                             config: old_val.config.as_ref().map(|old_config| v53::ClusterConfig {
    //                                 variant: old_config.variant.as_ref().map(|variant| match variant {
    //                                     v52::cluster_config::Variant::Unmanaged(_) => {
    //                                         v53::cluster_config::Variant::Unmanaged(v53::Empty {})
    //                                     }
    //                                     v52::cluster_config::Variant::Managed(c) => {
    //                                         v53::cluster_config::Variant::Managed(
    //                                             v53::cluster_config::ManagedCluster {
    //                                                 size: c.size.clone(),
    //                                                 replication_factor: c.replication_factor,
    //                                                 availability_zones: c.availability_zones.clone(),
    //                                                 logging: c
    //                                                     .logging
    //                                                     .as_ref()
    //                                                     .map(WireCompatible::convert),
    //                                                 idle_arrangement_merge_effort: c
    //                                                     .idle_arrangement_merge_effort
    //                                                     .as_ref()
    //                                                     .map(WireCompatible::convert),
    //                                                 disk: c.disk,
    //                                                 optimizer_feature_overrides: c
    //                                                     .optimizer_feature_overrides
    //                                                     .iter()
    //                                                     .map(WireCompatible::convert)
    //                                                     .collect(),
    //                                                 schedule: Some(v53::ClusterScheduleOptionValue{value: Some(v53::cluster_schedule_option_value::Value::Manual(Default::default()))}), // The new field
    //                                             },
    //                                         )
    //                                     }
    //                                 }),
    //                             }),
    //                         }),
    //                     },
    //                 ),
    //             ),
    //         };
    //
    //         Some(MigrationAction::Update(current, new))
    //     })
    //     .collect()

    Vec::new() /////////////////////////////

}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        let v52 = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Cluster(
                v52::state_update_kind::Cluster {
                    key: Some(v52::ClusterKey {
                        id: Some(v52::ClusterId {
                            value: Some(v52::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v52::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v52::RoleId {
                            value: Some(v52::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v52::ClusterConfig {
                            variant: Some(v52::cluster_config::Variant::Managed(
                                v52::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v52::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v52::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    idle_arrangement_merge_effort: Some(v52::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: vec![
                                        v52::OptimizerFeatureOverride {
                                            name: "feature1".to_string(),
                                            value: "false".to_string(),
                                        },
                                        v52::OptimizerFeatureOverride {
                                            name: "feature2".to_string(),
                                            value: "true".to_string(),
                                        },
                                    ],
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let v53 = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Cluster(
                v53::state_update_kind::Cluster {
                    key: Some(v53::ClusterKey {
                        id: Some(v53::ClusterId {
                            value: Some(v53::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v53::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v53::RoleId {
                            value: Some(v53::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v53::ClusterConfig {
                            variant: Some(v53::cluster_config::Variant::Managed(
                                v53::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v53::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v53::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    idle_arrangement_merge_effort: Some(v53::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: vec![
                                        v53::OptimizerFeatureOverride {
                                            name: "feature1".to_string(),
                                            value: "false".to_string(),
                                        },
                                        v53::OptimizerFeatureOverride {
                                            name: "feature2".to_string(),
                                            value: "true".to_string(),
                                        },
                                    ],
                                    schedule: Some(v53::ClusterScheduleOptionValue {
                                        value: Some(
                                            v53::cluster_schedule_option_value::Value::Manual(
                                                Default::default(),
                                            ),
                                        ),
                                    }), // The new field
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let actions = crate::durable::upgrade::v52_to_v53::upgrade(vec![v52.clone()]);

        match &actions[..] {
            [MigrationAction::Update(old, new)] => {
                assert_eq!(old, &v52);
                assert_eq!(new, &v53);
            }
            o => panic!("expected single MigrationAction::Update, got {:?}", o),
        }
    }
}
