// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Derive insights for plans.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_expr::{AccessStrategy, Id, MirRelationExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_repr::explain::ExprHumanizer;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::Statement;
use mz_sql::names::Aug;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use serde::Serialize;
use mz_controller_types::ClusterId;
use crate::TimestampContext;
use crate::catalog::Catalog;
use crate::coord::peek::FastPathPlan;
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::OptimizerConfig;
use crate::session::SessionMeta;

/// Information needed to compute PlanInsights.
#[derive(Debug)]
pub struct PlanInsightsContext {
    pub stmt: Option<Statement<Aug>>,
    pub raw_expr: HirRelationExpr,
    pub catalog: Arc<Catalog>,
    // Snapshots of all user compute instances.
    //
    // TODO: Avoid populating this if not needed. Maybe make this a method that can return a
    // ComputeInstanceSnapshot for a given cluster.
    /////////// todo: can we remove this and maybe some other fields too?
    pub compute_instances: BTreeMap<String, ComputeInstanceSnapshot>,
    pub target_instance: String,
    pub metrics: OptimizerMetrics,
    pub finishing: RowSetFinishing,
    pub optimizer_config: OptimizerConfig,
    pub session: SessionMeta,
    pub timestamp_context: TimestampContext<Timestamp>,
    pub view_id: GlobalId,
    pub index_id: GlobalId,
    pub enable_re_optimize: bool,
    pub fast_path_limit: bool, /////////// todo maybe better name
    pub fast_path_clusters: Vec<(ClusterId, GlobalId, GlobalId)>, ////////// todo struct or comment (cluster, index_id, on_id)
}

/// Insights about an optimized plan.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanInsights {
    /// Collections imported by the plan.
    ///
    /// Each key is the ID of an imported collection, and each value contains
    /// further insights about each collection and how it is used by the plan.
    pub imports: BTreeMap<String, ImportInsights>,
    /// If this plan is not fast path, this is the map of cluster names to indexes that would render
    /// this as fast path. That is: if this query were run on the cluster of the key, it would be
    /// fast because it would use the index of the value.
    pub fast_path_clusters: BTreeMap<String, Option<FastPathCluster>>,
    /// For the current cluster, whether adding a LIMIT <= this will result in a fast path.
    pub fast_path_limit: Option<usize>,
    /// Names of persist sources over which a count(*) is done.
    pub persist_count: Vec<Name>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FastPathCluster {
    pub index: Name,   ///////// todo: new instead of pub?
    pub on: Name,
}

/// Insights about an imported collection in a plan.
#[derive(Clone, Debug, Serialize)]
pub struct ImportInsights {
    /// The full name of the imported collection.
    pub name: Name,
    /// The type of the imported collection.
    #[serde(rename = "type")]
    pub ty: ImportType,
}

/// The type of an imported collection.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportType {
    /// A compute collection--i.e., an index.
    Compute,
    /// A storage collection: a table, source, or materialized view.
    Storage,
}

/// The name of a collection.
#[derive(Debug, Clone, Serialize)]
pub struct Name {
    /// The database name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// The schema name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// The item name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<String>,
}

pub fn plan_insights(
    humanizer: &dyn ExprHumanizer,
    global_plan: Option<DataflowDescription<OptimizedMirRelationExpr>>,
    fast_path_plan: Option<FastPathPlan>,
) -> Option<PlanInsights> {
    match (global_plan, fast_path_plan) {
        (None, None) => None,
        (None | Some(_), Some(fast_path_plan)) => {
            Some(fast_path_insights(humanizer, fast_path_plan))
        }
        (Some(global_plan), None) => Some(global_insights(humanizer, global_plan)),
    }
}

fn fast_path_insights(humanizer: &dyn ExprHumanizer, plan: FastPathPlan) -> PlanInsights {
    let mut insights = PlanInsights::default();
    match plan {
        FastPathPlan::Constant { .. } => (),
        FastPathPlan::PeekExisting(_, id, _, _) => {
            add_import_insights(&mut insights, humanizer, id, ImportType::Compute)
        }
        FastPathPlan::PeekPersist(id, _, _) => {
            add_import_insights(&mut insights, humanizer, id, ImportType::Storage)
        }
    }
    insights
}

fn global_insights(
    humanizer: &dyn ExprHumanizer,
    plan: DataflowDescription<OptimizedMirRelationExpr>,
) -> PlanInsights {
    let mut insights = PlanInsights::default();
    for (id, _) in plan.source_imports {
        add_import_insights(&mut insights, humanizer, id, ImportType::Storage)
    }
    for (id, _) in plan.index_imports {
        add_import_insights(&mut insights, humanizer, id, ImportType::Compute)
    }
    for BuildDesc { plan, .. } in plan.objects_to_build {
        // Search for a count(*) over a persist read.
        plan.visit_pre(|expr| {
            let MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } = expr
            else {
                return;
            };
            if !group_key.is_empty() {
                return;
            }
            let MirRelationExpr::Project { input, outputs } = &**input else {
                return;
            };
            if !outputs.is_empty() {
                return;
            }
            let MirRelationExpr::Get {
                id: Id::Global(id),
                access_strategy: AccessStrategy::Persist,
                ..
            } = &**input
            else {
                return;
            };
            let [aggregate] = aggregates.as_slice() else {
                return;
            };
            if !aggregate.is_count_asterisk() {
                return;
            }
            let name = structured_name(humanizer, *id);
            insights.persist_count.push(name);
        });
    }
    insights
}

fn add_import_insights(
    insights: &mut PlanInsights,
    humanizer: &dyn ExprHumanizer,
    id: GlobalId,
    ty: ImportType,
) {
    insights.imports.insert(
        id.to_string(),
        ImportInsights {
            name: structured_name(humanizer, id),
            ty,
        },
    );
}

////////// todo: maybe move it instead of pub
pub fn structured_name(humanizer: &dyn ExprHumanizer, id: GlobalId) -> Name {
    let mut parts = humanizer.humanize_id_parts(id).unwrap_or(Vec::new());
    Name {
        item: parts.pop(),
        schema: parts.pop(),
        database: parts.pop(),
    }
}
