// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::{AggregateExpr, AggregateFunc, ColumnOrder, LagLeadType, MapFilterProject, MirRelationExpr, MirScalarExpr, TableFunc, VariadicFunc};
use mz_expr::visit::Visit;
use mz_repr::{ColumnType, Datum, Row, ScalarType};

/// Window function calls, which we want to render specially (e.g., use prefix sum), or transform
/// away (e.g., ROW_NUMBER <= k to TopK).
pub enum WindowFuncCall {
    LagLead(LagLead),
}

pub struct LagLead {
    lag_or_lead: LagLeadType,
    // arguments:
    expr: MirScalarExpr,
    offset: MirScalarExpr,
    default: MirScalarExpr,
    // shared window stuff (maybe move up into a shared struct):
    partition_by: Vec<MirScalarExpr>,
    order_by_exprs: Vec<MirScalarExpr>,
    column_orders: Vec<ColumnOrder>,
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
                                            let order_by_exprs = &exprs[1..];
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
                                                    let window_func_args = match &exprs[1] {
                                                        MirScalarExpr::CallVariadic {func: VariadicFunc::RecordCreate {..}, exprs} => {
                                                            Some(exprs.clone())
                                                        }
                                                        MirScalarExpr::Literal(lit, lit_type) => {
                                                            // Can happen when the RecordCreate gets const-folded,
                                                            // i.e., when the window function arguments are constants.
                                                            match lit {
                                                                Ok(lit_row) => {
                                                                    match (lit_row.unpack_first(), lit_type) {
                                                                        (
                                                                            Datum::List(datum_list),
                                                                            ColumnType {
                                                                                scalar_type:
                                                                                ScalarType::Record {
                                                                                    fields: field_types,
                                                                                    ..
                                                                                },
                                                                                ..
                                                                            },
                                                                        ) => {
                                                                            Some(datum_list
                                                                                .iter()
                                                                                .zip(field_types)
                                                                                .map(|(datum, (_, typ))| {
                                                                                    MirScalarExpr::Literal(
                                                                                        Ok(Row::pack_slice(&[datum])),
                                                                                        typ.clone(),
                                                                                    )
                                                                                })
                                                                                .collect()
                                                                            )
                                                                        },
                                                                        _ => {
                                                                            assert!(false);
                                                                            None
                                                                        }
                                                                    }
                                                                }
                                                                Err(_) => None, // window func arguments const folded to an error
                                                            }
                                                        }
                                                        e => {
                                                            assert!(false);
                                                            None
                                                        }
                                                    };

                                                    match window_func_args {
                                                        None => None,
                                                        Some(window_func_args) => {
                                                            match agg {
                                                                AggregateExpr {
                                                                    func: AggregateFunc::LagLead { order_by, lag_lead },
                                                                    ..
                                                                } => {
                                                                    Some(WindowFuncCall::LagLead(LagLead{
                                                                        lag_or_lead: *lag_lead,
                                                                        expr: window_func_args[0].clone(),
                                                                        offset: window_func_args[1].clone(),
                                                                        default: window_func_args[2].clone(),
                                                                        partition_by: group_key.clone(),
                                                                        order_by_exprs: order_by_exprs.to_vec(),
                                                                        column_orders: order_by.clone(),
                                                                    }))
                                                                }
                                                                _ => None,
                                                            }
                                                        }
                                                    }

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

/// Compares the result of `match_window_func_mir_pattern`, which tries to parse window function
/// call patterns with the results of a trivial window function call search.
pub fn test_match_window_func_mir_pattern(expr: &MirRelationExpr) {
    let mut window_func_count = 0;
    #[allow(deprecated)]
    expr.visit_post_nolimit(&mut |e| {
        match e {
            MirRelationExpr::Reduce {aggregates, ..} => {
                if aggregates.iter().any(|agg| agg.is_window_func()) {
                    window_func_count += 1;
                }
                assert!(aggregates.iter().filter(|agg| agg.is_window_func()).count() <= 1); //todo: handle it when it's more instead of asserting
            }
            _ => {}
        }
    });

    let mut pattern_count = 0;
    #[allow(deprecated)]
    expr.visit_post_nolimit(&mut |e| {
        let window_func_call = match_window_func_mir_pattern(e);
        if window_func_call.is_some() {
            pattern_count += 1;
        }
    });

    // pattern_count can be more than window_func_count, because the pattern might match at
    // different starting points in the top MFP.
    assert_eq!(window_func_count > 0, pattern_count > 0);
}
