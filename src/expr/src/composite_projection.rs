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
//! In most situations, Materialize represents a projection simply by a `Vec<usize>`, which
//! expresses that the projection is taking the given columns in the given order. However, a column
//! might contain a record (or other compound type), in which case `CompositeProjection` can express
//! that only certain fields of the record are needed.
//!
//! `CompositeProjection`'s purpose is to be used in `ProjectionPushdown`, but it needs to be
//! defined here, in `mz-expr`, because the /// todo /// scalar function relies on it, and scalar
//! functions are defined here.
//!
//! (The most important case where it's useful for `ProjectionPushdown` to dig into composite types
//! is when it wants to push a projection through the window function MIR pattern.)
//!
//! This struct relies on some other structs:
//!
//! `CompositeReference` points to a part of a row or a record, including the ability to dig into
//! composite types. For example, it can reference to field 3 of the record that is at column 2 of
//! a row.
//!
//! `CompositeConstructor` contains instructions for how a `CompositeProjection` should build an
//! output column or a field of an output record.
//!
//! Finally, `CompositeProjection` is just an ordered list of `CompositeConstructor`s, each of which
//! builds one column of the output row.
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
//!
//! `CompositeConstructor` has the invariant that fields can't come out or go into a list, i.e.,
//! a `CompositeReference` should always go through exactly the same list that is being built by
//! the `CompositeConstructor` that contains the `CompositeReference`.

use std::collections::BTreeSet;

use mz_repr::ColumnName;

use crate::{MirRelationExpr, MirScalarExpr, VariadicFunc};

#[derive(Debug)]
pub struct CompositeProjection {
    constructors: Vec<CompositeConstructor>,
}

#[derive(Debug)]
enum CompositeConstructor {
    Simple(CompositeReference),
    Record(Vec<(ColumnName, CompositeConstructor)>),
    List(Box<CompositeConstructor>),
}

/// For example, simply referring to col 3 of a row is
/// `CompositeReference::Row(3, CompositeReference::Simple)`
/// (This works both when col 3 is a simple type, and also when it's a complex type, but we don't
/// want to dig into it.)
#[derive(Debug)]
enum CompositeReference {
    Simple,
    Row(usize, Box<CompositeReference>),
    Record(usize, Box<CompositeReference>),
    List(Box<CompositeReference>),
}

#[derive(Debug)]
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
                CompositeConstructor::Simple(CompositeReference::Row(col, inner_ref)) if matches!(*inner_ref, CompositeReference::Simple) => {
                    projections.push(col);
                }
                // Otherwise, we need to build a non-trivial expression, put it in the Map, and put
                // a reference it into the Project.
                _ => {
                    map_exprs.push(ctor.to_mir());
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
    fn to_mir(self) -> MirScalarExpr {
        match self {
            CompositeConstructor::Simple(reference) => {
                reference.to_mir()
            }
            CompositeConstructor::Record(fields) => {
                let (field_names, ctors): (_, Vec<_>) = fields.into_iter().unzip();
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::RecordCreate {
                        field_names
                    },
                    exprs: ctors.into_iter().map(|ctor| ctor.to_mir()).collect()
                }
            }
            CompositeConstructor::List(elem_ctor) => {
                let elem_transform = elem_ctor.map_references(&mut CompositeReference::peel_list);
                todo!()
            }
        }
    }

    /// Yields a new `CompositeConstructor` by applying the given function to all
    /// `CompositeReference`s contained in the given `CompositeConstructor`.
    fn map_references<F>(self, f: &mut F) -> CompositeConstructor
    where
        F: FnMut(CompositeReference) -> CompositeReference,
    {
        match self {
            CompositeConstructor::Simple(r) => CompositeConstructor::Simple(f(r)),
            CompositeConstructor::Record(fields) => {
                CompositeConstructor::Record(
                    fields.into_iter().map(|(field_name, ctor): (ColumnName, CompositeConstructor)| {
                        (field_name, ctor.map_references(f))
                    }).collect()
                )
            }
            CompositeConstructor::List(ctor) => {
                CompositeConstructor::List(Box::new(ctor.map_references(f)))
            }
        }
    }
}

impl CompositeReference {
    fn to_mir(self) -> MirScalarExpr {
        todo!()
    }

    /// Dig down into the `CompositeReference` until we reach a `List`, and return the reference in
    /// that `List`.
    fn peel_list(self) -> CompositeReference {
        match self {
            CompositeReference::Simple => {
                // It shouldn't happen that we reach a leaf before reaching a `List`.
                panic!("CompositeConstructor invariant violated: a CompositeReference in a CompositeConstructor::List references outside the list");
            },
            CompositeReference::Row(_col_ind, r)
            | CompositeReference::Record(_col_ind, r) => {
                // Dig further down.
                r.peel_list()
            }
            CompositeReference::List(r) => *r
        }
    }
}
