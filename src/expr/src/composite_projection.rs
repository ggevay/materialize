// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `CompositeProjection` is a projection that can look into composite types, such as a record or a
//! list. (Later, we could extend this to be able to look into json.)
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
//!   composite types. For example, it can reference to field 2 of the record that is at column 3 of
//!   a row.
//! - `CompositeConstructor` contains instructions for how a `CompositeProjection` should build an
//!   output column or a field of an output record.
//!
//! Finally, `CompositeProjection` is just an ordered list of `CompositeConstructor`s, each of which
//! builds one column of the output row.
//!
//! When it comes to `List`s, `CompositeProjection` is meant to be able to drop or reorganize
//! information only within each list element individually. In other words, `CompositeProjection` is
//! _not_ meant to be able to move information between list elements or make lists appear or
//! disappear or make list elements appear or disappear. (`CompositeProjection`s involving lists
//! arise when pushing a projection through a `FlatMap unnest_list(...)`: the projection was
//! originally operating on each row, now it needs to operate on each list element.)
//!
//! Additionally, the struct `CompositeReferenceSet` is defined here. In some places where
//! `ProjectionPushdown` used to have a `Vec<usize>`, it now has a either a `CompositeProjection`
//! or a `CompositeReferenceSet`. It would be tempting to simplify it to use a
//! `CompositeReferenceSet` everywhere, but then it couldn't express the pushdown of a reordering of
//! columns. This would mean that often an extra `Mfp` stage would appear at the roots of plans, to
//! perform a reordering that could have been performed somewhere deeper in the plan by a
//! `Project` that needs to exist anyway, because it needs to also project away columns.
//! (See the slt changes in
//! <https://github.com/ggevay/materialize/commit/cbd3eec578e16515b99c6c9073d7b9bca1702459>.)

use mz_ore::soft_assert_no_log;
use std::collections::BTreeSet;

use mz_repr::ColumnName;

use crate::func::RecordGet;
use crate::{MirRelationExpr, MirScalarExpr, UnaryFunc, VariadicFunc};

#[derive(Debug, Clone)]
pub struct CompositeProjection {
    constructors: Vec<CompositeConstructor>,
}

#[derive(Debug, Clone)]
enum CompositeConstructor {
    // Construct a non-composite value.
    Simple(CompositeReference),
    // Construct a `Record` with the given field names. How to construct the value of each field is
    // specified by further `CompositeConstructor`s.
    Record(Vec<(ColumnName, CompositeConstructor)>),
    // Construct a `List`.
    List {
        // How to get the list from the input.
        list_reference: CompositeReference,
        // How to construct an element of the output list from an element of the input list.
        elem_constructor: Box<CompositeConstructor>,
    },
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
#[derive(Debug, Clone)]
enum CompositeReference {
    Simple,
    Row(usize, Box<CompositeReference>),
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
                // a reference it into the Project.
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
    /// References in `self` which are not hidden behind a `CompositeConstructor::List` should all
    /// start with `CompositeReference::Row`!
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
            CompositeConstructor::List {
                list_reference,
                elem_constructor,
            } => {
                todo!()
                // asszem itt kb. az kell, hogy a ListMap-et meghivni
                // `elem_ctor.to_mir_on_expr(#0)` fuggvennyel (fontos hogy on_expr)
                // Es a ListMap-et meg ugy megirni, hogy a list elemeit berakja egy row #0-jara, es `eval`-ozza a megadott `MirScalarExpr`-t minden elemre.
                // De mi a ListMap list argumentje? `list_reference`
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
            CompositeConstructor::List {
                list_reference,
                elem_constructor: _,
            } => {
                matches!(list_reference, CompositeReference::Row(..))
                // Don't descend into `elem_constructor`.
            }
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
    /// `CompositeReference`s therein.
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
            CompositeConstructor::List {
                list_reference,
                elem_constructor,
            } => CompositeConstructor::List {
                list_reference: f(list_reference),
                elem_constructor: Box::new(elem_constructor.map_references(f)),
            },
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
