// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regression tests for stack safety of the derived trait implementations of
//! [`HirRelationExpr`] and [`HirScalarExpr`].
//!
//! See `src/expr/tests/deep_plan.rs` for the MIR counterpart and the shape of
//! the tests. The recursive fields of the HIR enums are `CheckedBox` and
//! `CheckedVec` from `mz_ore::stack`, which makes the derived clone, eq,
//! hash, debug, serde, and drop walks stack-safe.

use std::hash::{DefaultHasher, Hash, Hasher};

use mz_ore::stack::CheckedBox;
use mz_repr::SqlRelationType;
use mz_sql::plan::{HirRelationExpr, HirScalarExpr};

/// The stack size for the test threads.
const TEST_STACK: usize = 4 << 20;

/// The depth of the test trees.
const DEPTH: usize = 100_000;

/// Runs `f` on a thread with a small stack and propagates its panic, if any.
fn on_small_stack<F: FnOnce() + Send + 'static>(f: F) {
    mz_ore::stack::run_on_small_stack(TEST_STACK, f).expect("no panic");
}

/// Builds a `DEPTH`-deep `HirRelationExpr` chain that alternates `Negate`
/// (a `CheckedBox` edge) and `Union` (recursing through an element of the
/// `CheckedVec` of inputs) levels. Built iteratively.
fn build_deep_relation(depth: usize) -> HirRelationExpr {
    let typ = SqlRelationType::new(vec![]);
    let mut expr = HirRelationExpr::constant(vec![], typ.clone());
    for i in 0..depth {
        expr = if i % 2 == 0 {
            HirRelationExpr::Negate {
                input: CheckedBox::new(expr),
            }
        } else {
            HirRelationExpr::Union {
                base: CheckedBox::new(HirRelationExpr::constant(vec![], typ.clone())),
                inputs: vec![expr].into(),
            }
        };
    }
    expr
}

/// Builds a `DEPTH`-deep `HirScalarExpr` chain that alternates `not()`
/// (a `CheckedBox` edge through `CallUnary`) and `variadic_and` (recursing
/// through an element of the `CheckedVec` of arguments) levels. Built
/// iteratively.
fn build_deep_scalar(depth: usize) -> HirScalarExpr {
    let mut expr = HirScalarExpr::literal_true();
    for i in 0..depth {
        expr = if i % 2 == 0 {
            expr.not()
        } else {
            HirScalarExpr::variadic_and(vec![expr, HirScalarExpr::literal_true()])
        };
    }
    expr
}

fn hash<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

#[mz_ore::test]
fn test_deep_hir_relation_clone_eq_hash_debug_drop() {
    on_small_stack(|| {
        let expr = build_deep_relation(DEPTH);
        let clone = expr.clone();
        assert!(expr == clone);
        assert_eq!(hash(&expr), hash(&clone));
        let rendered = format!("{expr:?}");
        assert!(rendered.starts_with("Union"));
        // Plain drops through the guarded Drop impls.
        drop(expr);
        drop(clone);
    });
}

#[mz_ore::test]
fn test_deep_hir_scalar_clone_eq_hash_debug_drop() {
    on_small_stack(|| {
        let expr = build_deep_scalar(DEPTH);
        let clone = expr.clone();
        assert!(expr == clone);
        assert_eq!(hash(&expr), hash(&clone));
        let rendered = format!("{expr:?}");
        assert!(rendered.starts_with("CallVariadic"));
        drop(expr);
        drop(clone);
    });
}
