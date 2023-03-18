// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::{AggregateExpr, AggregateFunc, MapFilterProject, MirRelationExpr, MirScalarExpr, TableFunc, VariadicFunc};
use mz_expr::visit::Visit;

/// Window function calls, which we want to render specially (e.g., use prefix sum), or transform
/// away (e.g., ROW_NUMBER <= k to TopK).
pub enum WindowFuncCall {
    Lag1(Lag1),
}

// Offset=1, default=NULL
pub struct Lag1 {
    expr: MirScalarExpr,
}

pub fn match_window_func_mir_pattern(expr: &MirRelationExpr) -> Option<(MapFilterProject, WindowFuncCall)> {

    let (top_mfp, below_the_top_mfp) = MapFilterProject::extract_from_expression(expr);
    let window_func_call = match below_the_top_mfp {
        MirRelationExpr::FlatMap {func: TableFunc::UnnestList {..}, input, exprs} => {
            // Check that the argument of the UnnestList is `#0`
            assert_eq!(exprs.len(), 1);
            match exprs[0] {
                MirScalarExpr::Column(0) => {
                    // Continue with the pattern
                    let (mfp2, below_mfp2) = MapFilterProject::extract_from_expression(input);
                    // Check that mfp2 is a projection yielding one column. (might be identity mfp)
                    // This check can fail if, for example, ReduceElision removed the Reduce
                    // that has the window function, and then everything is different.
                    if mfp2.expressions.is_empty() && mfp2.predicates.is_empty() && mfp2.projection.len() == 1 {
                        // Check for the Reduce below the Project
                        match below_mfp2 {
                            MirRelationExpr::Reduce { aggregates, group_key, input, .. } => {
                                assert_eq!(mfp2.projection[0], group_key.len()); // check that the Project gets the column after the group key
                                // Check that there is only one aggregation.
                                assert_eq!(aggregates.len(), 1); // actually, we should handle it if there is more
                                let agg = &aggregates[0];
                                if agg.is_window_func() {
                                    assert!(!agg.distinct);
                                    match &agg.expr {
                                        MirScalarExpr::CallVariadic {func: VariadicFunc::RecordCreate {..}, exprs} => {
                                            let _order_by_exprs = &exprs[1..];
                                            match &exprs[0] {
                                                MirScalarExpr::CallVariadic {func: VariadicFunc::RecordCreate {..}, exprs} => {
                                                    assert_eq!(exprs.len(), 2);
                                                    match &exprs[0] {
                                                        MirScalarExpr::CallVariadic {func: VariadicFunc::RecordCreate {..}, exprs} => {
                                                            assert_eq!(exprs.len(), input.arity());
                                                            assert!(exprs.iter().enumerate().all(|(i, e)| matches!(e, MirScalarExpr::Column(c) if *c == i)));
                                                        }
                                                        _ => {assert!(false);}
                                                    }
                                                    let _window_func_args = match &exprs[1] {
                                                        MirScalarExpr::CallVariadic {func: VariadicFunc::RecordCreate {..}, exprs} => {
                                                            //todo
                                                            ////////////pattern_count += 1;
                                                            exprs.clone()
                                                        }
                                                        MirScalarExpr::Literal(..) => {
                                                            // Can happen when the RecordCreate gets const-folded,
                                                            // i.e., when the window function arguments are constants.
                                                            // todo
                                                            ////////////pattern_count += 1;
                                                            Vec::new()
                                                        }
                                                        e => {
                                                            assert!(false);
                                                            Vec::new()
                                                        }
                                                    };

                                                    // match agg {
                                                    //     AggregateExpr {
                                                    //         func: AggregateFunc::LagLead {..},
                                                    //         ..
                                                    //     } => {
                                                    //         Some(WindowFuncCall::Lag1(Lag1{expr: _window_func_args[0].clone()}))
                                                    //     }
                                                    //     _ => None,
                                                    // }

                                                    //Some(WindowFuncCall::Lag1(Lag1{expr: _window_func_args[0].clone()}))

                                                    Some(WindowFuncCall::Lag1(Lag1{expr: MirScalarExpr::column(0)}))

                                                }
                                                _ => {
                                                    assert!(false);
                                                    None
                                                }
                                            }
                                        }
                                        _ => {
                                            assert!(false);
                                            None
                                        }
                                    }
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => {
                    assert!(false);
                    None
                }
            }
        }
        _ => None,
    };

    match window_func_call {
        Some(window_function_call) => Some((top_mfp, window_function_call)),
        None => None,
    }
}