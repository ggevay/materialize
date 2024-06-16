// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pushes column removal down through other operators.
//!
//! This action improves the quality of the query by
//! reducing the width of data in the dataflow. It determines the unique
//! columns an expression depends on, and pushes a projection onto only
//! those columns down through child operators.
//!
//! A `MirRelationExpr::Project` node is actually three transformations in one.
//! 1) Projection - removes columns.
//! 2) Permutation - reorders columns.
//! 3) Repetition - duplicates columns.
//!
//! This action handles these three transformations like so:
//! 1) Projections are pushed as far down as possible.
//! 2) Permutations are pushed as far down as is convenient.
//! 3) Repetitions are not pushed down at all.
//!
//! Some comments have been inherited from the `Demand` transform.
//!
//! Note that this transform is one that can operate across views in a dataflow
//! and thus currently exists outside of both the physical and logical
//! optimizers.

use std::collections::{BTreeMap, BTreeSet};

use itertools::{zip_eq, Itertools};
use mz_expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT, VariadicFunc};
use mz_ore::assert_none;
use mz_ore::soft_assert_no_log;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

use crate::{TransformCtx, TransformError};

/// Pushes projections down through other operators.
#[derive(Debug)]
pub struct ProjectionPushdown {
    recursion_guard: RecursionGuard,
}

impl Default for ProjectionPushdown {
    fn default() -> Self {
        Self {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for ProjectionPushdown {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for ProjectionPushdown {
    // This method is only used during unit testing.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "projection_pushdown")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let result = self.action(
            relation,
            &(0..relation.arity()).collect(),
            &mut BTreeMap::new(),
        );
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl ProjectionPushdown {
    /// Pushes the `desired_projection` down through `relation`.
    ///
    /// This action transforms `relation` to a `MirRelationExpr` equivalent to
    /// `relation.project(desired_projection)`.
    ///
    /// `desired_projection` is expected to consist of unique columns.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        desired_projection: &Vec<usize>,
        gets: &mut BTreeMap<Id, BTreeSet<usize>>,
    ) -> Result<(), TransformError> {
        soft_assert_no_log!(desired_projection.iter().all_unique());
        self.checked_recur(|_| {
            // First, try to push the desired projection down through `relation`.
            // In the process `relation` is transformed to a `MirRelationExpr`
            // equivalent to `relation.project(actual_projection)`.
            // There are three reasons why `actual_projection` may differ from
            // `desired_projection`:
            // 1) `relation` may need one or more columns that is not contained in
            //    `desired_projection`.
            // 2) `relation` may not be able to accommodate certain permutations.
            //    For example, `MirRelationExpr::Map` always appends all
            //    newly-created columns to the end.
            // 3) Nothing can be pushed through a leaf node. If `relation` is a leaf
            //    node, `actual_projection` will always be `(0..relation.arity())`.
            // Then, if `actual_projection` and `desired_projection` differ, we will
            // add a project around `relation`.
            let actual_projection = match relation {
                MirRelationExpr::Constant { .. } => (0..relation.arity()).collect(),
                MirRelationExpr::Get { id, .. } => {
                    gets.entry(*id)
                        .or_insert_with(BTreeSet::new)
                        .extend(desired_projection.iter().cloned());
                    (0..relation.arity()).collect()
                }
                MirRelationExpr::Let { id, value, body } => {
                    // Let harvests any requirements of get from its body,
                    // and pushes the sorted union of the requirements at its value.
                    let id = Id::Local(*id);
                    let prior = gets.insert(id, BTreeSet::new());
                    self.action(body, desired_projection, gets)?;
                    let desired_value_projection = gets.remove(&id).unwrap();
                    if let Some(prior) = prior {
                        gets.insert(id, prior);
                    }
                    let desired_value_projection =
                        desired_value_projection.into_iter().collect::<Vec<_>>();
                    self.action(value, &desired_value_projection, gets)?;
                    let new_type = value.typ();
                    self.update_projection_around_get(
                        body,
                        &BTreeMap::from_iter(std::iter::once((
                            id,
                            (desired_value_projection, new_type),
                        ))),
                    );
                    desired_projection.clone()
                }
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    limits: _,
                    body,
                } => {
                    // Determine the recursive IDs in this LetRec binding.
                    let rec_ids = MirRelationExpr::recursive_ids(ids, values);

                    // Seed the gets map with empty demand for each non-recursive ID.
                    for id in ids.iter().filter(|id| !rec_ids.contains(id)) {
                        let prior = gets.insert(Id::Local(*id), BTreeSet::new());
                        assert_none!(prior);
                    }

                    // Descend into the body with the supplied desired_projection.
                    self.action(body, desired_projection, gets)?;
                    // Descend into the values in reverse order.
                    for (id, value) in zip_eq(ids.iter().rev(), values.iter_mut().rev()) {
                        let desired_projection = if rec_ids.contains(id) {
                            // For recursive IDs: request all columns.
                            let columns = 0..value.arity();
                            columns.collect::<Vec<_>>()
                        } else {
                            // For non-recursive IDs: request the gets entry.
                            let columns = gets.get(&Id::Local(*id)).unwrap();
                            columns.iter().cloned().collect::<Vec<_>>()
                        };
                        self.action(value, &desired_projection, gets)?;
                    }

                    // Update projections around gets of non-recursive IDs.
                    let mut updates = BTreeMap::new();
                    for (id, value) in zip_eq(ids.iter(), values.iter_mut()) {
                        // Update the current value.
                        self.update_projection_around_get(value, &updates);
                        // If this is a non-recursive ID, add an entry to the
                        // updates map for subsequent values and the body.
                        if !rec_ids.contains(id) {
                            let new_type = value.typ();
                            let new_proj = {
                                let columns = gets.remove(&Id::Local(*id)).unwrap();
                                columns.iter().cloned().collect::<Vec<_>>()
                            };
                            updates.insert(Id::Local(*id), (new_proj, new_type));
                        }
                    }
                    // Update the body.
                    self.update_projection_around_get(body, &updates);

                    // Remove the entries for all ids (don't restrict only to
                    // non-recursive IDs here for better hygene).
                    for id in ids.iter() {
                        gets.remove(&Id::Local(*id));
                    }

                    // Return the desired projection (leads to a no-op in the
                    // projection handling logic after this match statement).
                    desired_projection.clone()
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    ..
                } => {
                    let input_mapper = JoinInputMapper::new(inputs);

                    let mut columns_to_pushdown =
                        desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                    // Each equivalence class imposes internal demand for columns.
                    for equivalence in equivalences.iter() {
                        for expr in equivalence.iter() {
                            expr.support_into(&mut columns_to_pushdown);
                        }
                    }

                    // Populate child demands from external and internal demands.
                    let new_columns =
                        input_mapper.split_column_set_by_input(columns_to_pushdown.iter());

                    // Recursively indicate the requirements.
                    for (input, inp_columns) in inputs.iter_mut().zip(new_columns) {
                        let inp_columns = inp_columns.into_iter().collect::<Vec<_>>();
                        self.action(input, &inp_columns, gets)?;
                    }

                    reverse_permute(
                        equivalences.iter_mut().flat_map(|e| e.iter_mut()),
                        columns_to_pushdown.iter(),
                    );

                    columns_to_pushdown.into_iter().collect()
                }
                MirRelationExpr::FlatMap { input, func, exprs } => {

                    // Lehet, hogy nem kene erolkodni a CompositeProjection-ben a listak kezelesevel,
                    // hanem egyszeruen itt at lehetne ugrani a listas reszt:
                    // - Ugyebar fokeppen azert nehez a ComopsiteProjection-ben kezelni a listakat, mert
                    //   kene az a higher-order map fn, ami viszont azert bajos, mert at kene nezni az osszes scalar visitationt az egesz kodban.
                    // - Az alabb lathato ketfele mintat kene kezelni. (Leellenoriztem a window_funcs.slt-re, hogy nem fordul elo mas. (Persze manualisan osszerakva elofordulhat mas, de az nem erdekes.))
                    //   - Sot, a list_create utan FlatMap UnnestList mintat el is lehetne tuntetni teljesen (akar vhol mashol)
                    //     - Vhol kezelni kene (vagy itt, vagy mashol eltuntetni teljesen), mert kulonben behoznank egy nemmonotonitast: ha unique key van, akkor elakadna a ProjectionPushdown
                    // - Az egesz window funct pattern bonyolult lenne, mert az unnest_list utan mindenfele dolgok tortenhetnek:
                    //   Nem biztos, hogy egy MFP szedi szet a rekordot, hanem pl. bemehet ujabb reduce-ba, vagy barmi masba ami scalar expr-t var
                    //   - (Bar amugy volt az a terv, hogy non-trivialis expr csak MFP-ben lehetne. De ez rovid tavon nem tortenik vszeg.)

                    if matches!(func, mz_expr::TableFunc::UnnestList {..}) {
                        let reduce = matches!(**input, MirRelationExpr::Reduce {..});
                        let map_list_create = match (**input).clone() {
                            MirRelationExpr::Map {scalars, ..} => {
                                scalars.len() == 1 && matches!(scalars[0], MirScalarExpr::CallVariadic {func: VariadicFunc::ListCreate {..}, ..})
                            }
                            _ => false
                        };
                        if !reduce && !map_list_create {
                            println!(">>>>>>>>>>>>>>>>>>>>>>>>>>\n{}\n", input.pretty());
                            panic!()
                        }
                        // assert!(
                        //     matches!(**input, MirRelationExpr::Reduce {..})
                        // );
                    }

                    let inner_arity = input.arity();
                    // A FlatMap which returns zero rows acts like a filter
                    // so we always need to execute it
                    let mut columns_to_pushdown =
                        desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                    for expr in exprs.iter() {
                        expr.support_into(&mut columns_to_pushdown);
                    }
                    columns_to_pushdown.retain(|c| *c < inner_arity);

                    reverse_permute(exprs.iter_mut(), columns_to_pushdown.iter());
                    let columns_to_pushdown = columns_to_pushdown.into_iter().collect::<Vec<_>>();
                    self.action(input, &columns_to_pushdown, gets)?;
                    // The actual projection always has the newly-created columns at
                    // the end.
                    let mut actual_projection = columns_to_pushdown;
                    for c in 0..func.output_type().arity() {
                        actual_projection.push(inner_arity + c);
                    }
                    actual_projection
                }
                MirRelationExpr::Filter { input, predicates } => {
                    let mut columns_to_pushdown =
                        desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                    for predicate in predicates.iter() {
                        predicate.support_into(&mut columns_to_pushdown);
                    }
                    reverse_permute(predicates.iter_mut(), columns_to_pushdown.iter());
                    let columns_to_pushdown = columns_to_pushdown.into_iter().collect::<Vec<_>>();
                    self.action(input, &columns_to_pushdown, gets)?;
                    columns_to_pushdown
                }
                MirRelationExpr::Project { input, outputs } => {
                    // Combine `outputs` with `desired_projection`.
                    *outputs = desired_projection.iter().map(|c| outputs[*c]).collect();

                    let unique_outputs = outputs.iter().map(|i| *i).collect::<BTreeSet<_>>();
                    if outputs.len() == unique_outputs.len() {
                        // Push down the project as is.
                        self.action(input, outputs, gets)?;
                        *relation = input.take_dangerous();
                    } else {
                        // Push down only the unique elems in `outputs`.
                        let columns_to_pushdown = unique_outputs.into_iter().collect::<Vec<_>>();
                        reverse_permute_columns(outputs.iter_mut(), columns_to_pushdown.iter());
                        self.action(input, &columns_to_pushdown, gets)?;
                    }

                    desired_projection.clone()
                }
                MirRelationExpr::Map { input, scalars } => {
                    let arity = input.arity();
                    // contains columns whose supports have yet to be explored
                    let mut actual_projection =
                        desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                    for (i, scalar) in scalars.iter().enumerate().rev() {
                        if actual_projection.contains(&(i + arity)) { /////////// todo: we have to note when only a field of it is requested
                            scalar.support_into(&mut actual_projection);
                        }
                    }
                    *scalars = (0..scalars.len())
                        .filter_map(|i| {
                            if actual_projection.contains(&(i + arity)) {
                                Some(scalars[i].clone())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    reverse_permute(scalars.iter_mut(), actual_projection.iter());
                    self.action(
                        input,
                        &actual_projection
                            .iter()
                            .filter(|c| **c < arity)
                            .map(|c| *c)
                            .collect(),
                        gets,
                    )?;
                    actual_projection.into_iter().collect()
                }
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic: _,
                    expected_group_size: _,
                } => {
                    let mut columns_to_pushdown = BTreeSet::new();
                    // Group keys determine aggregation granularity and are
                    // each crucial in determining aggregates and even the
                    // multiplicities of other keys.
                    for k in group_key.iter() {
                        k.support_into(&mut columns_to_pushdown)
                    }

                    for index in (0..aggregates.len()).rev() {
                        if !desired_projection.contains(&(group_key.len() + index)) {
                            aggregates.remove(index);
                        } else {
                            // No obvious requirements on aggregate columns.
                            // A "non-empty" requirement, I guess?
                            aggregates[index]
                                .expr
                                .support_into(&mut columns_to_pushdown)
                        }
                    }

                    reverse_permute(
                        itertools::chain!(
                            group_key.iter_mut(),
                            aggregates.iter_mut().map(|a| &mut a.expr)
                        ),
                        columns_to_pushdown.iter(),
                    );

                    self.action(
                        input,
                        &columns_to_pushdown.into_iter().collect::<Vec<_>>(),
                        gets,
                    )?;
                    let mut actual_projection =
                        desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                    actual_projection.extend(0..group_key.len());
                    actual_projection.into_iter().collect()
                }
                MirRelationExpr::TopK {
                    input,
                    group_key,
                    order_key,
                    limit,
                    ..
                } => {
                    // Group and order keys and limit support must be retained, as
                    // they define which rows are retained.
                    let mut columns_to_pushdown =
                        desired_projection.iter().cloned().collect::<BTreeSet<_>>();
                    columns_to_pushdown.extend(group_key.iter().cloned());
                    columns_to_pushdown.extend(order_key.iter().map(|o| o.column));
                    if let Some(limit) = limit.as_ref() {
                        // Strictly speaking not needed because the
                        // `limit` support should be a subset of the
                        // `group_key` support, but we don't want to
                        // take this for granted here.
                        limit.support_into(&mut columns_to_pushdown);
                    }
                    // If the `TopK` does not have any new column demand, just push
                    // down the desired projection. Otherwise, push down the sorted
                    // column demand.
                    let columns_to_pushdown =
                        if columns_to_pushdown.len() == desired_projection.len() {
                            desired_projection.clone()
                        } else {
                            columns_to_pushdown.into_iter().collect::<Vec<_>>()
                        };
                    reverse_permute_columns(
                        itertools::chain!(
                            group_key.iter_mut(),
                            order_key.iter_mut().map(|o| &mut o.column),
                        ),
                        columns_to_pushdown.iter(),
                    );
                    reverse_permute(limit.iter_mut(), columns_to_pushdown.iter());
                    self.action(input, &columns_to_pushdown, gets)?;
                    columns_to_pushdown
                }
                MirRelationExpr::Negate { input } => {
                    self.action(input, desired_projection, gets)?;
                    desired_projection.clone()
                }
                MirRelationExpr::Union { base, inputs } => {
                    self.action(base, desired_projection, gets)?;
                    for input in inputs {
                        self.action(input, desired_projection, gets)?;
                    }
                    desired_projection.clone()
                }
                MirRelationExpr::Threshold { input } => {
                    // Threshold requires all columns, as collapsing any distinct values
                    // has the potential to change how it thresholds counts. This could
                    // be improved with reasoning about distinctness or non-negativity.
                    let arity = input.arity();
                    self.action(input, &(0..arity).collect(), gets)?;
                    (0..arity).collect()
                }
                MirRelationExpr::ArrangeBy { input, keys: _ } => {
                    // Do not push the project past the ArrangeBy.
                    // TODO: how do we handle key sets containing column references
                    // that are not demanded upstream?
                    let arity = input.arity();
                    self.action(input, &(0..arity).collect(), gets)?;
                    (0..arity).collect()
                }
            };
            let add_project = desired_projection != &actual_projection;
            if add_project {
                let mut projection_to_add = desired_projection.to_owned();
                reverse_permute_columns(projection_to_add.iter_mut(), actual_projection.iter());
                *relation = relation.take_dangerous().project(projection_to_add);
            }
            Ok(())
        })
    }

    /// When we push the `desired_value_projection` at `value`,
    /// the columns returned by `Get(get_id)` will change, so we need
    /// to permute `Project`s around `Get(get_id)`.
    pub fn update_projection_around_get(
        &self,
        relation: &mut MirRelationExpr,
        applied_projections: &BTreeMap<Id, (Vec<usize>, mz_repr::RelationType)>,
    ) {
        relation.visit_pre_mut(|e| {
            if let MirRelationExpr::Project { input, outputs } = e {
                if let MirRelationExpr::Get {
                    id: inner_id,
                    typ,
                    access_strategy: _,
                } = &mut **input
                {
                    if let Some((new_projection, new_type)) = applied_projections.get(inner_id) {
                        typ.clone_from(new_type);
                        reverse_permute_columns(outputs.iter_mut(), new_projection.iter());
                        if outputs.len() == new_projection.len()
                            && outputs.iter().enumerate().all(|(i, o)| i == *o)
                        {
                            *e = input.take_dangerous();
                        }
                    }
                }
            }
            // If there is no `Project` around a Get, all columns of
            // `Get(get_id)` are required. Thus, the columns returned by
            // `Get(get_id)` will not have changed, so no action
            // is necessary.
        });
    }
}

/// Applies the reverse of [MirScalarExpr.permute] on each expression.
///
/// `permutation` can be thought of as a mapping of column references from
/// `stateA` to `stateB`. [MirScalarExpr.permute] assumes that the column
/// references of the expression are in `stateA` and need to be remapped to
/// their `stateB` counterparts. This method assumes that the column
/// references are in `stateB` and need to be remapped to `stateA`.
///
/// The `outputs` field of [MirRelationExpr::Project] is a mapping from "after"
/// to "before". Thus, when lifting projections, you would permute on `outputs`,
/// but you need to reverse permute when pushing projections down.
fn reverse_permute<'a, I, J>(exprs: I, permutation: J)
where
    I: Iterator<Item = &'a mut MirScalarExpr>,
    J: Iterator<Item = &'a usize>,
{
    let reverse_col_map = permutation
        .enumerate()
        .map(|(idx, c)| (*c, idx))
        .collect::<BTreeMap<_, _>>();
    for expr in exprs {
        expr.permute_map(&reverse_col_map);
    }
}

/// Same as [reverse_permute], but takes column numbers as input.
fn reverse_permute_columns<'a, I, J>(columns: I, permutation: J)
where
    I: Iterator<Item = &'a mut usize>,
    J: Iterator<Item = &'a usize>,
{
    let reverse_col_map = permutation
        .enumerate()
        .map(|(idx, c)| (*c, idx))
        .collect::<BTreeMap<_, _>>();
    for c in columns {
        *c = reverse_col_map[c];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold_constants::FoldConstants;
    use crate::Transform;
    use crate::{typecheck, DataflowMetainfo, OptimizerFeatures};
    use mz_expr::composite_projection::{
        CompositeConstructor, CompositeProjection, CompositeReference,
    };
    use mz_repr::{ColumnName, ColumnType, Datum, Diff, RelationType, Row, RowArena, ScalarType};

    #[mz_ore::test]
    fn test_composite_projection_apply() {
        /// Calls `FoldConstants` on the given expr and returns the resulting constant
        /// or panics if we don't arrive at a constant.
        fn fold_to_constant(mut expr: MirRelationExpr) -> Vec<(Row, Diff)> {
            let fc = FoldConstants { limit: None };

            let features = OptimizerFeatures::default();
            let typecheck_ctx = typecheck::empty_context();
            let mut df_meta = DataflowMetainfo::default();
            let mut transform_ctx = TransformCtx::local(&features, &typecheck_ctx, &mut df_meta);

            fc.transform(&mut expr, &mut transform_ctx).unwrap();
            match expr {
                MirRelationExpr::Constant { rows: Ok(rows), .. } => {
                    return rows;
                }
                MirRelationExpr::Constant { rows: Err(e), .. } => {
                    panic!("FoldConstants has run into an EvalError: {}", e);
                }
                _ => {
                    panic!("FoldConstants hasn't arrived at a constant");
                }
            }
        }

        fn test(
            inp_data: Vec<Vec<Datum>>,
            inp_type: RelationType,
            proj: CompositeProjection,
            expected: Vec<Vec<Datum>>,
        ) {
            let inp_rows = inp_data
                .into_iter()
                .map(|row_datums| (Row::pack(row_datums), 1))
                .collect();
            let expected_rows: Vec<_> = expected
                .into_iter()
                .map(|row_datums| (Row::pack(row_datums), 1))
                .collect();

            let input_expr = proj.apply(MirRelationExpr::Constant {
                rows: Ok(inp_rows),
                typ: inp_type,
            });
            let result_rows = fold_to_constant(input_expr);
            assert_eq!(result_rows, expected_rows);
        }

        fn record<'a>(arena: &'a mut RowArena, datums: Vec<Datum<'a>>) -> Datum<'a> {
            arena.make_datum(|packer| packer.push_list(datums))
        }

        let mut arena = RowArena::new();
        let mut arena2 = RowArena::new();

        // Simple values in input.
        let inp_data1 = vec![vec![
            Datum::Int32(0),
            Datum::Int32(10),
            Datum::Int32(20),
            Datum::Int32(30),
        ]];
        let int32_type = ColumnType {
            scalar_type: ScalarType::Int32,
            nullable: true,
        };
        let inp_type1 = RelationType::new(vec![
            int32_type.clone(),
            int32_type.clone(),
            int32_type.clone(),
            int32_type.clone(),
        ]);

        // Build simple values only.
        test(
            inp_data1.clone(),
            inp_type1.clone(),
            CompositeProjection {
                constructors: vec![
                    CompositeConstructor::Simple(CompositeReference::Row(
                        3,
                        Box::new(CompositeReference::Simple),
                    )),
                    CompositeConstructor::Simple(CompositeReference::Row(
                        1,
                        Box::new(CompositeReference::Simple),
                    )),
                ],
            },
            vec![vec![Datum::Int32(30), Datum::Int32(10)]],
        );
        test(
            inp_data1.clone(),
            inp_type1.clone(),
            CompositeProjection {
                constructors: vec![],
            },
            vec![vec![]],
        );

        // Build a record from simple values.
        test(
            inp_data1.clone(),
            inp_type1.clone(),
            CompositeProjection {
                constructors: vec![
                    CompositeConstructor::Record(vec![
                        (
                            "a".into(),
                            CompositeConstructor::Simple(CompositeReference::Row(
                                3,
                                Box::new(CompositeReference::Simple),
                            )),
                        ),
                        (
                            "b".into(),
                            CompositeConstructor::Simple(CompositeReference::Row(
                                2,
                                Box::new(CompositeReference::Simple),
                            )),
                        ),
                    ]),
                    CompositeConstructor::Simple(CompositeReference::Row(
                        1,
                        Box::new(CompositeReference::Simple),
                    )),
                ],
            },
            vec![vec![
                record(&mut arena, vec![Datum::Int32(30), Datum::Int32(20)]),
                Datum::Int32(10),
            ]],
        );

        // Record in input.
        let inp_data2 = vec![vec![
            Datum::Int32(0),
            record(&mut arena, vec![Datum::Int32(10), Datum::Int32(20)]),
            Datum::Int32(30),
        ]];
        let inp_type2 = RelationType::new(vec![
            int32_type.clone(),
            ColumnType {
                scalar_type: ScalarType::Record {
                    fields: vec![
                        (ColumnName::from("a"), int32_type.clone()),
                        (ColumnName::from("b"), int32_type.clone()),
                    ],
                    custom_id: None,
                },
                nullable: true,
            },
            int32_type.clone(),
        ]);

        // Build simple values from record.
        test(
            inp_data2.clone(),
            inp_type2.clone(),
            CompositeProjection {
                constructors: vec![
                    CompositeConstructor::Simple(CompositeReference::Row(
                        2,
                        Box::new(CompositeReference::Simple),
                    )),
                    CompositeConstructor::Simple(CompositeReference::Row(
                        1,
                        Box::new(CompositeReference::Record(
                            0,
                            Box::new(CompositeReference::Simple),
                        )),
                    )),
                ],
            },
            vec![vec![Datum::Int32(30), Datum::Int32(10)]],
        );

        // Build a record from a record.
        test(
            inp_data2.clone(),
            inp_type2.clone(),
            CompositeProjection {
                constructors: vec![
                    CompositeConstructor::Simple(CompositeReference::Row(
                        2,
                        Box::new(CompositeReference::Simple),
                    )),
                    CompositeConstructor::Record(vec![
                        (
                            ColumnName::from("c"),
                            CompositeConstructor::Simple(CompositeReference::Row(
                                0,
                                Box::new(CompositeReference::Simple),
                            )),
                        ),
                        (
                            ColumnName::from("d"),
                            CompositeConstructor::Simple(CompositeReference::Row(
                                1,
                                Box::new(CompositeReference::Record(
                                    0,
                                    Box::new(CompositeReference::Simple),
                                )),
                            )),
                        ),
                    ]),
                ],
            },
            vec![vec![
                Datum::Int32(30),
                record(&mut arena2, vec![Datum::Int32(0), Datum::Int32(10)]),
            ]],
        );
    }
}
