// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regression tests for stack safety of the derived trait implementations of
//! [`MirRelationExpr`] and [`MirScalarExpr`].
//!
//! The recursive fields of the two enums are `CheckedBox`/`CheckedVec` from
//! `mz_ore::stack`, which guard clone, eq, hash, debug, serde, and drop with
//! `maybe_grow`. Each test builds a deep tree on a thread with a small stack,
//! where the same operations with plain `Box`/`Vec` fields overflow.

use std::hash::{DefaultHasher, Hash, Hasher};

use mz_expr::{MirRelationExpr, MirScalarExpr, UnaryFunc, func};
use mz_ore::stack::CheckedBox;
use mz_repr::{Datum, ReprRelationType, ReprScalarType, SqlScalarType};

/// The stack size for the test threads.
const TEST_STACK: usize = 4 << 20;

/// The depth of the test trees.
const DEPTH: usize = 100_000;

/// Runs `f` on a thread with a small stack and propagates its panic, if any.
fn on_small_stack<F: FnOnce() + Send + 'static>(f: F) {
    mz_ore::stack::run_on_small_stack(TEST_STACK, f).expect("no panic");
}

/// Builds a `DEPTH`-deep `MirRelationExpr` chain that alternates `Negate`
/// (a `CheckedBox` edge) and `Union` (recursing through an element of the
/// `CheckedVec` of inputs) levels. Built iteratively.
fn build_deep_relation(depth: usize) -> MirRelationExpr {
    let typ = ReprRelationType::new(vec![]);
    let mut expr = MirRelationExpr::constant(vec![], typ.clone());
    for i in 0..depth {
        expr = if i % 2 == 0 {
            MirRelationExpr::Negate {
                input: CheckedBox::new(expr),
            }
        } else {
            MirRelationExpr::Union {
                base: CheckedBox::new(MirRelationExpr::constant(vec![], typ.clone())),
                inputs: vec![expr].into(),
            }
        };
    }
    expr
}

/// Builds a `DEPTH`-deep `MirScalarExpr` chain that alternates `CallUnary`
/// (a `CheckedBox` edge) and `CallVariadic` (recursing through an element of
/// the `CheckedVec` of arguments) levels. Built iteratively.
fn build_deep_scalar(depth: usize) -> MirScalarExpr {
    let mut expr = MirScalarExpr::literal_ok(Datum::True, ReprScalarType::Bool);
    for i in 0..depth {
        expr = if i % 2 == 0 {
            expr.call_unary(UnaryFunc::Not(func::Not))
        } else {
            MirScalarExpr::call_variadic(func::variadic::And, vec![expr])
        };
    }
    expr
}

#[mz_ore::test]
fn test_deep_relation_clone_eq_hash_debug_drop() {
    on_small_stack(|| {
        let expr = build_deep_relation(DEPTH);
        let clone = expr.clone();
        assert!(expr == clone);
        assert_eq!(expr.hash_to_u64(), clone.hash_to_u64());
        let rendered = format!("{expr:?}");
        assert!(rendered.starts_with("Union"));
        // Plain drops. The guarded Drop of the recursive fields makes any eager
        // dismantling helper unnecessary.
        drop(expr);
        drop(clone);
    });
}

/// Builds a `DEPTH`-deep `MirScalarExpr` chain that recurses through the
/// `cast_expr` payload of `UnaryFunc::CastStringToList`, a recursive scalar
/// edge that hides inside the function payload rather than in the
/// `MirScalarExpr` enum itself. Built iteratively.
fn build_deep_cast_payload(depth: usize) -> MirScalarExpr {
    let mut expr = MirScalarExpr::column(0);
    for _ in 0..depth {
        expr = MirScalarExpr::CallUnary {
            func: UnaryFunc::CastStringToList(func::CastStringToList {
                return_ty: SqlScalarType::String,
                cast_expr: CheckedBox::new(expr),
            }),
            expr: CheckedBox::new(MirScalarExpr::column(0)),
        };
    }
    expr
}

#[mz_ore::test]
fn test_deep_cast_payload_clone_eq_hash_debug_drop() {
    on_small_stack(|| {
        let expr = build_deep_cast_payload(DEPTH);
        let clone = expr.clone();
        assert!(expr == clone);
        let hash = |e: &MirScalarExpr| {
            let mut h = DefaultHasher::new();
            e.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&expr), hash(&clone));
        let rendered = format!("{expr:?}");
        assert!(rendered.starts_with("CallUnary"));
        drop(expr);
        drop(clone);
    });
}

#[mz_ore::test]
fn test_deep_scalar_clone_eq_hash_debug_drop() {
    on_small_stack(|| {
        let expr = build_deep_scalar(DEPTH);
        let clone = expr.clone();
        assert!(expr == clone);
        let hash = |e: &MirScalarExpr| {
            let mut h = DefaultHasher::new();
            e.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&expr), hash(&clone));
        let rendered = format!("{expr:?}");
        assert!(rendered.starts_with("CallVariadic"));
        drop(expr);
        drop(clone);
    });
}
