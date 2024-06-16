// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `CompositeProjection` is a projection that can look into composite types, such as a record.
//! (Later, we could extend this to be able to project from inside a json or operate on each element
//! of a list or array.)
//!
//! Applying a `CompositeProjection` constructs a row involving composite types from an input row
//! that also involves composite types, dropping and/or reorganizing columns/fields in the process.
//!
//! In most situations, Materialize represents a projection simply by a `Vec<usize>`, which
//! expresses that the projection is taking the given columns in the given order. However, a column
//! might contain a record (or other compound type), in which case `CompositeProjection` is smarter
//! than a `Vec<usize>` in that it can express that only certain fields of the record are needed
//! and/or reorganize fields.
//!
//! `CompositeProjection`'s purpose is to be used in `ProjectionPushdown`, but it needs to be
//! defined here, in `mz-expr`, because the /// todo /// scalar function relies on it, and scalar
//! functions are defined here.
//!
//! (The most important case where it's useful for `ProjectionPushdown` to dig into composite types
//! is when it wants to push a projection through the window function MIR pattern.)
//!
//! This struct relies on some other structs:
//! - `CompositeReference` points to a part of a row or a record, including the ability to dig into
//!   composite types. For example, it can reference field 2 of the record that is at column 3 of
//!   a row.
//! - `CompositeConstructor` contains instructions for how a `CompositeProjection` should build an
//!   output column or a field of an output record.
//!
//! Finally, `CompositeProjection` is just an ordered list of `CompositeConstructor`s, each of which
//! builds one column of the output row.
//!
//! Additionally, the struct `CompositeReferenceSet` is defined here. In some places where
//! `ProjectionPushdown` used to have a `Vec<usize>`, it now has either a `CompositeProjection`
//! or a `CompositeReferenceSet`. It would be tempting to simplify it to use a
//! `CompositeReferenceSet` everywhere, but then it couldn't express the pushdown of a reordering of
//! columns. This would mean that often an extra `Mfp` stage would appear at the roots of plans, to
//! perform a reordering that could have been performed somewhere deeper in the plan by a
//! `Project` that needs to exist anyway, because it needs to also project away columns.
//! (See the would be slt changes in
//! <https://github.com/ggevay/materialize/commit/cbd3eec578e16515b99c6c9073d7b9bca1702459>.)

use std::collections::BTreeSet;

use mz_ore::soft_assert_no_log;
use mz_ore::stack::RecursionLimitError;
use mz_repr::ColumnName;

use crate::func::RecordGet;
use crate::{MirRelationExpr, MirScalarExpr, UnaryFunc, VariadicFunc};
use crate::visit::Visit;

#[derive(Debug, Clone)]
pub struct CompositeProjection {
    pub constructors: Vec<CompositeConstructor>,
}

#[derive(Debug, Clone)]
pub enum CompositeConstructor {
    // Construct a non-composite value.
    Simple(CompositeReference),
    // Construct a `Record` with the given field names. How to construct the value of each field is
    // specified by further `CompositeConstructor`s.
    Record(Vec<(ColumnName, CompositeConstructor)>),
}

/// Examples:
///
/// Referring to col 3 of a row:
/// (This works both when col 3 is a simple type, and also when it's a complex type, but we don't
/// want to dig into it.)
/// `CompositeReference::Row(3, CompositeReference::Simple)`
/// Converted to `MirScalarExpr`:
/// `#3`
///
/// Referring to field 2 of the record that is at column 3 of a row:
/// `CompositeReference::Row(3, CompositeReference::Record(2, CompositeReference::Simple))`
/// Converted to `MirScalarExpr`:
/// `record_get[2](#3)`
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub enum CompositeReference {
    /// The referenced value is treated as a simple type. (It might actually be a composite type,
    /// but we are not digging into it.)
    Simple,
    /// The referenced value is found at the given column index of a row.
    Row(usize, Box<CompositeReference>),
    /// The referenced value is found at the given field index of a record.
    Record(usize, Box<CompositeReference>),
}

#[derive(Debug, Clone)]
pub struct CompositeReferenceSet {
    refs: BTreeSet<CompositeReference>,
}

impl CompositeProjection {
    /// Converts the projection to MIR and puts it on top of a given `MirRelationExpr`.
    pub fn apply(self, mut expr: MirRelationExpr) -> MirRelationExpr {
        // We do a Map to construct complex columns (if needed), and then do a Project.
        // We build the Map and Project at the same time, while going through `self.constructors`.
        let mut arity_before_project = expr.arity();
        let mut projections = Vec::new();
        let mut map_exprs = Vec::new();
        for ctor in self.constructors {
            match ctor {
                // If the column to construct is a simple type, and we are constructing it from a
                // simple column of a row, then we can represent this ctor simply in the Project.
                CompositeConstructor::Simple(CompositeReference::Row(col, inner_ref))
                    if matches!(*inner_ref, CompositeReference::Simple) =>
                {
                    projections.push(col);
                }
                // Otherwise, we need to build a non-trivial expression, put it in the Map, and put
                // a reference to it into the Project.
                _ => {
                    map_exprs.push(ctor.to_mir_on_row());
                    projections.push(arity_before_project);
                    arity_before_project += 1;
                }
            }
        }
        if !map_exprs.is_empty() {
            expr = expr.map(map_exprs);
        }
        expr.project(projections)
    }
}

impl CompositeConstructor {
    /// Turns the `CompositeConstructor` into a `MirScalarExpr` that operates on an input row.
    ///
    /// References in `self` should all start with `CompositeReference::Row`!
    fn to_mir_on_row(self) -> MirScalarExpr {
        soft_assert_no_log!(self.valid_on_row());
        self.to_mir(&None)
    }

    /// Turns the `CompositeConstructor` into a `MirScalarExpr` that is a composition of the
    /// original input expression and constructing a value from the result of the given input
    /// expression.
    ///
    /// References in `self` shouldn't involve `CompositeReference::Row`!
    fn to_mir_on_expr(self, input: MirScalarExpr) -> MirScalarExpr {
        soft_assert_no_log!(self.valid_on_expr());
        self.to_mir(&Some(input))
    }

    /// Turns the `CompositeConstructor` into a `MirScalarExpr` that either operates on a row or on
    /// the result of the given `MirScalarExpr`.
    fn to_mir(self, input: &Option<MirScalarExpr>) -> MirScalarExpr {
        match self {
            CompositeConstructor::Simple(reference) => reference.to_mir(input.clone()),
            CompositeConstructor::Record(fields) => {
                let (field_names, ctors): (_, Vec<_>) = fields.into_iter().unzip();
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::RecordCreate { field_names },
                    exprs: ctors.into_iter().map(|ctor| ctor.to_mir(input)).collect(),
                }
            }
        }
    }

    fn valid_on_row(&self) -> bool {
        match &self {
            CompositeConstructor::Simple(r) => matches!(r, CompositeReference::Row(..)),
            CompositeConstructor::Record(fields) => fields
                .iter()
                .map(|(_field_name, ctor)| ctor.valid_on_row())
                .reduce(|a, b| a && b).unwrap_or(true),
        }
    }

    fn valid_on_expr(&self) -> bool {
        let mut valid = true;
        self.clone().map_references(&mut |r| {
            valid |= !matches!(r, CompositeReference::Row(..));
            r
        });
        valid
    }

    /// Yields a new `CompositeConstructor` by applying the given function to all
    /// `CompositeReference`s therein. (It's _not_ lazy.)
    fn map_references<F>(self, f: &mut F) -> CompositeConstructor
    where
        F: FnMut(CompositeReference) -> CompositeReference,
    {
        match self {
            CompositeConstructor::Simple(r) => CompositeConstructor::Simple(f(r)),
            CompositeConstructor::Record(fields) => CompositeConstructor::Record(
                fields
                    .into_iter()
                    .map(|(field_name, ctor): (ColumnName, CompositeConstructor)| {
                        (field_name, ctor.map_references(f))
                    })
                    .collect(),
            ),
        }
    }
}

impl CompositeReference {
    /// Turns the `CompositeReference` into a `MirScalarExpr` that operates on an input row.
    fn to_mir_on_row(self) -> MirScalarExpr {
        match self {
            CompositeReference::Row(col_ind, inner_ref) => {
                inner_ref.to_mir_on_expr(MirScalarExpr::Column(col_ind))
            }
            _ => panic!("to_mir_on_row called on a non-Row CompositeReference"),
        }
    }

    /// Turns the `CompositeReference` into a `MirScalarExpr` that is a composition of the
    /// original input expression and constructing a value from the result of the given input
    /// expression.
    fn to_mir_on_expr(self, input: MirScalarExpr) -> MirScalarExpr {
        match self {
            CompositeReference::Simple => input,
            CompositeReference::Row(..) => {
                panic!("to_mir_on_expr called on a CompositeReference::Row");
            }
            CompositeReference::Record(field_ind, inner_ref) => {
                inner_ref.to_mir_on_expr(MirScalarExpr::CallUnary {
                    func: UnaryFunc::RecordGet(RecordGet(field_ind)),
                    expr: Box::new(input),
                })
            }
        }
    }

    /// Turns the `CompositeReference` into a `MirScalarExpr` that either operates on a row or on
    /// the result of the given `MirScalarExpr`.
    fn to_mir(self, input: Option<MirScalarExpr>) -> MirScalarExpr {
        match input {
            None => self.to_mir_on_row(),
            Some(input) => self.to_mir_on_expr(input),
        }
    }
}

impl CompositeReferenceSet {
    pub fn new() -> Self {
        CompositeReferenceSet {refs: BTreeSet::new()}
    }

    pub fn add_support_of(&mut self, expr: &MirScalarExpr, ref_above_expr: &CompositeReference) -> Result<(), RecursionLimitError> {
        expr.visit_pre_with_context(
            Vec::new(),
            &mut |record_gets, expr| {
                match expr {
                    MirScalarExpr::CallUnary {
                        func: UnaryFunc::RecordGet(RecordGet(field)),
                        expr: _,
                    } => {
                        let mut cloned = record_gets.clone();
                        cloned.push(*field);
                        cloned
                    },
                    _ => {
                        // forget record_gets
                        Vec::new()
                    }
                }
            },
            &mut |record_gets, expr| {
                match expr {
                    MirScalarExpr::Column(c) => {
                        let mut r = ref_above_expr.clone();
                        for field in record_gets.into_iter().rev() {
                            r = CompositeReference::Record(*field, Box::new(r));
                        }
                        r = CompositeReference::Row(*c, Box::new(r));
                        self.refs.insert(r);
                    },
                    _ => {},
                }
            }
        )
        /////// todo: normalize?
        // - 0. make unique? Will be needed for reverse_permute?
        // - 1. when both a full record and some of its fields are requested, forget about the fields and just keep the full record?
        // - 2. when all fields of a record all requested, just request the record itself?

        ////// todo: rewrite in terms of visit_pre_post?:
        // - match record_get, and go inwards while there are record_gets
        // - if there are not record_gets at all, return None (to just continue the visitation with all children)
        // - if in the middle there is something else than a column ref, then just vist it and forget about the record_gets
        // - if in the middle there is a column ref, then create CompositeReference::Record, and don't visit any children
    }

    pub fn add_support_of_rewrite(&mut self, expr: &MirScalarExpr, ref_above_expr: &CompositeReference) -> Result<(), RecursionLimitError> {
        expr.visit_pre_post(
            &mut |expr| {
                match expr {
                    MirScalarExpr::CallUnary {
                        func: UnaryFunc::RecordGet(RecordGet(field)),
                        expr,
                    } => {
                        let mut record_gets = vec![field];
                        // while let MirScalarExpr::CallUnary {
                        //     func: UnaryFunc::RecordGet(RecordGet(field)),
                        //     expr,
                        // } = *expr {
                        //
                        // }
                        None /////////////////
                    },
                    _ => {
                        None
                    }
                }
            },
            &mut |expr| {}
        )


        /////// todo: copy todo from above
    }
}
