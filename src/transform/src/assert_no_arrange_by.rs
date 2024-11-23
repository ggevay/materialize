// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Assert that the given MIR plan doesn't have any `ArrangeBy`s.

use mz_expr::MirRelationExpr;
use mz_ore::soft_panic_or_log;
use crate::{Transform, TransformCtx};

/// See above, at the module level.
#[derive(Debug)]
pub struct AssertNoArrangeBy;

impl Transform for AssertNoArrangeBy {
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "assert_no_arrange_by")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.visit_pre(|expr| {
            match expr {
                MirRelationExpr::ArrangeBy { .. } => {
                    ////////////////////////////////////////soft_panic_or_log!("AssertNoArrangeBy found an arrangement in plan {}", relation.pretty());
                }
                _ => {}
            }
        });

        // Intentionally no call to `trace_plan` here, because we don't change the plann.

        Ok(())
    }
}
