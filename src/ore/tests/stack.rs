// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for the stack-safe `CheckedBox`/`CheckedVec` wrappers.
//!
//! Each test runs on a thread with a deliberately small stack, so that a
//! recursion without the wrappers' `maybe_grow` guards would overflow. The
//! tree depth is chosen to overflow such a stack by a wide margin when the
//! test enum's recursive fields are plain `Box`/`Vec` instead of the
//! wrappers.

use std::hash::{DefaultHasher, Hash, Hasher};

use mz_ore::stack::{CheckedBox, CheckedVec, run_on_small_stack};

/// The stack size for the test threads.
const TEST_STACK: usize = 4 << 20;

/// The depth of the test trees.
const DEPTH: usize = 100_000;

/// A small recursive enum whose recursive edges alternate between a
/// `CheckedBox` field and a `CheckedVec` element hop, mirroring how the IR
/// enums mix `Box` and `Vec` children.
#[derive(
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    serde::Serialize,
    serde::Deserialize
)]
enum TestExpr {
    Leaf(u64),
    Unary(CheckedBox<TestExpr>),
    Nary(CheckedVec<TestExpr>),
}

/// Builds a `DEPTH`-deep chain that alternates `Unary` (a `CheckedBox` edge)
/// and `Nary` (a `CheckedVec` edge) levels. Built iteratively, so
/// construction itself needs no stack guard.
fn build_deep(depth: usize) -> TestExpr {
    let mut expr = TestExpr::Leaf(42);
    for i in 0..depth {
        expr = if i % 2 == 0 {
            TestExpr::Unary(CheckedBox::new(expr))
        } else {
            TestExpr::Nary(vec![expr, TestExpr::Leaf(7)].into())
        };
    }
    expr
}

/// Runs `f` on a thread with a small stack and propagates its panic, if any.
fn on_small_stack<F: FnOnce() + Send + 'static>(f: F) -> std::thread::Result<()> {
    run_on_small_stack(TEST_STACK, f)
}

#[mz_ore::test]
fn test_deep_clone_eq_hash_debug_drop() {
    on_small_stack(|| {
        let expr = build_deep(DEPTH);

        // Clone, then Eq between the two deep values.
        let clone = expr.clone();
        assert!(expr == clone);

        // Hash both, guarded per level, and expect equal hashes.
        let mut h1 = DefaultHasher::new();
        expr.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        clone.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());

        // Debug formatting walks the whole tree.
        let rendered = format!("{expr:?}");
        assert!(rendered.starts_with("Nary"));
        assert!(rendered.contains("Leaf(42)"));

        // Plain drops of both deep values, through the guarded Drop impls.
        drop(expr);
        drop(clone);
    })
    .expect("no panic");
}

/// A twin of [`TestExpr`] with plain `Box`/`Vec` recursive fields, for
/// asserting that the wrappers leave the serde and Debug output unchanged.
#[derive(
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    serde::Serialize,
    serde::Deserialize
)]
enum PlainExpr {
    Leaf(u64),
    Unary(Box<PlainExpr>),
    Nary(Vec<PlainExpr>),
}

#[mz_ore::test]
fn test_deep_serialize_and_format_identity() {
    on_small_stack(|| {
        // Deep serialization through the guarded Serialize impls. (Deep
        // deserialization is not exercised because serde_json's own
        // deserializer recursion is unguarded and its recursion limit
        // rejects deep inputs. The Deserialize guard mirrors Serialize.)
        let expr = build_deep(DEPTH);
        let json = serde_json::to_string(&expr).expect("serializes");
        assert!(json.len() > DEPTH);
        drop(expr);

        // On a shallow tree, the wrappers must produce byte-identical serde
        // and Debug output to the plain Box/Vec twin, and must roundtrip.
        let checked = build_deep(10);
        let plain = {
            let mut expr = PlainExpr::Leaf(42);
            for i in 0..10 {
                expr = if i % 2 == 0 {
                    PlainExpr::Unary(Box::new(expr))
                } else {
                    PlainExpr::Nary(vec![expr, PlainExpr::Leaf(7)])
                };
            }
            expr
        };
        let checked_json = serde_json::to_string(&checked).expect("serializes");
        let plain_json = serde_json::to_string(&plain).expect("serializes");
        assert_eq!(checked_json, plain_json);
        assert_eq!(format!("{checked:?}"), format!("{plain:?}"));
        // The two enums have identical shapes and variant order, so
        // identical hash streams mean the wrappers hash like Box/Vec.
        let mut h1 = DefaultHasher::new();
        checked.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        plain.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
        let roundtripped: TestExpr = serde_json::from_str(&checked_json).expect("deserializes");
        assert!(checked == roundtripped);
    })
    .expect("no panic");
}

#[mz_ore::test]
fn test_deep_drop_during_panic_unwind() {
    // Build a deep value on a small-stack thread and panic while it is
    // live. The unwind drops the deep value through the guarded Drop impl,
    // which verifies that maybe_grow works during unwind.
    let result = on_small_stack(|| {
        let expr = build_deep(DEPTH);
        if matches!(expr, TestExpr::Leaf(_)) {
            unreachable!();
        }
        panic!("deliberate panic with deep value live");
    });
    let panic_payload = result.expect_err("panic must propagate");
    let msg = panic_payload
        .downcast_ref::<&str>()
        .copied()
        .unwrap_or_default();
    assert_eq!(msg, "deliberate panic with deep value live");
}
