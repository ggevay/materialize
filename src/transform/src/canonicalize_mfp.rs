// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Canonicalizes MFPs, e.g., performs CSE on the scalar expressions, eliminates identity MFPs.
//!
//! This transform takes a sequence of Maps, Filters, and Projects and
//! canonicalizes it to a sequence like this:
//! | Map
//! | Filter
//! | Project
//!
//! As part of canonicalizing, this transform looks at the Map-Filter-Project
//! subsequence and identifies common `ScalarExpr` expressions across and within
//! expressions that are arguments to the Map-Filter-Project. It reforms the
//! `Map-Filter-Project` subsequence to build each distinct expression at most
//! once and to re-use expressions instead of re-evaluating them.
//!
//! The re-use policy at the moment is severe and re-uses everything.
//! It may be worth considering relations of this if it results in more
//! busywork and less efficiency, but the wins can be substantial when
//! expressions re-use complex subexpressions.

use crate::{IndexOracle, TransformArgs};
use mz_expr::visit::VisitChildren;
use mz_expr::{MapFilterProject, MirRelationExpr};
use mz_expr::canonicalize::canonicalize_predicates;

/// Canonicalizes MFPs and performs common sub-expression elimination.
#[derive(Debug)]
pub struct CanonicalizeMfp;

impl crate::Transform for CanonicalizeMfp {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "canonicalize_mfp")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = self.action(relation, args.indexes);
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

impl CanonicalizeMfp {
    fn action(
        &self,
        relation: &mut MirRelationExpr,
        indexes: &dyn IndexOracle,
    ) -> Result<(), crate::TransformError> {
        let mut mfp = MapFilterProject::extract_non_errors_from_expr_mut(relation);
        relation.try_visit_mut_children(|e| self.action(e, indexes))?;
        CanonicalizeMfp::canonicalize_predicates(&mut mfp, relation);
        mfp.optimize(); // Optimize MFP, e.g., perform CSE
        Self::rebuild_mfp(mfp, relation);
        Ok(())
    }

    /// Call [mz_expr::canonicalize::canonicalize_predicates] on each of the predicates in the MFP.
    pub fn canonicalize_predicates(mfp: &mut MapFilterProject, relation: &MirRelationExpr) {
        let (map, mut predicates, project) = mfp.as_map_filter_project();
        let typ_after_map = relation.clone().map(map.clone()).typ();
        canonicalize_predicates(&mut predicates, &typ_after_map.column_types);
        // Rebuild the MFP with the new predicates.
        *mfp = MapFilterProject::new(mfp.input_arity)
            .map(map)
            .filter(predicates)
            .project(project);
    }

    /// Translate the `MapFilterProject` into actual Map, Filter, Project operators. Add these on
    /// top of the given `relation` expression.
    pub fn rebuild_mfp(mfp: MapFilterProject, relation: &mut MirRelationExpr) {
        if !mfp.is_identity() {
            let (map, filter, project) = mfp.as_map_filter_project();
            let total_arity = mfp.input_arity + map.len();
            if !map.is_empty() {
                *relation = relation.take_dangerous().map(map);
            }
            if !filter.is_empty() {
                *relation = relation.take_dangerous().filter(filter);
                crate::fusion::filter::Filter.action(relation);
            }
            if project.len() != total_arity || !project.iter().enumerate().all(|(i, o)| i == *o) {
                *relation = relation.take_dangerous().project(project);
            }
        }
    }
}
