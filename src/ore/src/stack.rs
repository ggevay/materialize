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

//! Stack management utilities.

#[cfg(feature = "tracing")]
use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
#[cfg(feature = "tracing")]
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(feature = "tracing")]
use tracing::error;

/// The red zone is the amount of stack space that must be available on the
/// current stack in order for [`maybe_grow`] to call the supplied closure
/// without allocating a new stack.
///
/// We use a much larger red zone in debug builds because several functions have
/// been observed to have 32KB+ stack frames when compiled without
/// optimizations. In particular, match statements on large enums are
/// problematic, because *each arm* of the match statement gets its own
/// dedicated stack space. For example, consider the following function:
///
/// ```ignore
/// fn big_stack(input: SomeEnum) {
///     match input {
///         SomeEnum::Variant1 => {
///             let a_local = SomeBigType::new();
///         }
///         SomeEnum::Variant2 => {
///             let b_local = SomeBigType::new();
///         }
///         // ...
///         SomeEnum::Variant10 => {
///             let z_local = SomeBigType::new();
///         }
///     }
/// }
/// ```
///
/// In debug builds, the compiler will generate a stack frame that contains
/// space for 10 separate copies of `SomeBigType`. This can quickly result in
/// massive stack frames for perfectly reasonable code.
pub const STACK_RED_ZONE: usize = {
    #[cfg(debug_assertions)]
    {
        1024 << 10 // 1024KiB
    }
    #[cfg(not(debug_assertions))]
    {
        64 << 10 // 64KiB
    }
};

/// The size of any freshly allocated stacks. It was chosen to match the default
/// stack size for threads in Rust.
///
/// The default stack size is larger in debug builds to correspond to the
/// larger [`STACK_RED_ZONE`].
pub const STACK_SIZE: usize = {
    #[cfg(debug_assertions)]
    {
        16 << 20 // 16MiB
    }
    #[cfg(not(debug_assertions))]
    {
        2 << 20 // 2 MiB
    }
};

/// Grows the stack if necessary before invoking `f`.
///
/// This function is intended to be called at manually instrumented points in a
/// program where arbitrarily deep recursion is known to happen. This function
/// will check to see if it is within `STACK_RED_ZONE` bytes of the end of the
/// stack, and if so it will allocate a new stack of at least `STACK_SIZE`
/// bytes.
///
/// The closure `f` is guaranteed to run on a stack with at least
/// `STACK_RED_ZONE` bytes, and it will be run on the current stack if there's
/// space available.
///
/// It is generally better to use [`CheckedRecursion`] to enforce a limit on the
/// stack growth. Not all recursive code paths support returning errors,
/// however, in which case unconditionally growing the stack with this function
/// is still preferable to panicking.
#[inline(always)]
pub fn maybe_grow<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    stacker::maybe_grow(STACK_RED_ZONE, STACK_SIZE, f)
}

/// The recursion limit used by optimizer (MIR) traversals, and the largest of
/// the recursion limits in the query-processing pipeline.
///
/// The limits form deliberate tiers. Each layer of the pipeline may only
/// produce trees that fit into the budget of the layers after it, so the
/// limits grow along the pipeline:
///
/// * mz-sql-parser: 128 for nested expressions (crate-local
///   `RECURSION_LIMIT`), 1024 for flat expression chains, which the parser
///   handles iteratively but later layers recurse over.
/// * mz-sql planning: 1024 (`PLANNER_RECURSION_LIMIT` in `mz_sql::plan`).
/// * optimizer/MIR: 2048 (this constant, also re-exported as
///   `mz_expr::RECURSION_LIMIT`).
///
/// The headroom of this last tier over the planner's exists because the
/// linear representation of some frequently occurring query fragments makes
/// MIR trees deeper than the HIR trees they are lowered from, for example
/// long chains of `Let` bindings and of `CallBinary` calls with associative
/// functions such as `+`.
pub const RECURSION_LIMIT: usize = 2048;

/// A trait for types which support bounded recursion to prevent stack overflow.
///
/// The rather odd design of this trait allows checked recursion to be added to
/// existing mutually recursive functions without threading an explicit `depth:
/// &mut usize` parameter through each function. As long as there is an
/// existing context structure, or if the mutually recursive functions are
/// methods on a context structure, the [`RecursionGuard`] can be embedded
/// inside this existing structure.
///
/// # Examples
///
/// Consider a simple expression evaluator:
///
/// ```
/// # use std::collections::BTreeMap;
///
/// enum Expr {
///     Var { name: String },
///     Add { left: Box<Expr>, right: Box<Expr> },
/// }
///
/// struct Evaluator {
///     vars: BTreeMap<String, i64>,
/// }
///
/// impl Evaluator {
///     fn eval(&mut self, expr: &Expr) -> i64 {
///         match expr {
///             Expr::Var { name } => self.vars[name],
///             Expr::Add { left, right } => self.eval(left) + self.eval(right),
///         }
///     }
/// }
/// ```
///
/// Calling `eval` could overflow the stack and crash with a sufficiently large
/// `expr`. This is the situation `CheckedRecursion` is designed to solve, like
/// so:
///
/// ```
/// # use std::collections::BTreeMap;
/// # enum Expr {
/// #     Var { name: String },
/// #     Add { left: Box<Expr>, right: Box<Expr> },
/// # }
/// use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
///
/// struct Evaluator {
///     vars: BTreeMap<String, i64>,
///     recursion_guard: RecursionGuard,
/// }
///
/// impl Evaluator {
///     fn eval(&mut self, expr: &Expr) -> Result<i64, RecursionLimitError> {
///         // ADDED: call to `self.checked_recur`.
///         self.checked_recur_mut(|e| match expr {
///             Expr::Var { name } => Ok(e.vars[name]),
///             Expr::Add { left, right } => Ok(e.eval(left)? + e.eval(right)?),
///         })
///     }
/// }
///
/// impl CheckedRecursion for Evaluator {
///     fn recursion_guard(&self) -> &RecursionGuard {
///         &self.recursion_guard
///     }
/// }
/// ```
pub trait CheckedRecursion {
    /// Extracts a reference to the recursion guard embedded within the type.
    fn recursion_guard(&self) -> &RecursionGuard;

    /// Checks whether it is safe to recur and calls `f` if so.
    ///
    /// If the recursion limit for the recursion guard returned by
    /// [`CheckedRecursion::recursion_guard`] has been reached, returns a
    /// `RecursionLimitError`. Otherwise, it will call `f`, possibly growing the
    /// stack if necessary.
    ///
    /// Calls to this function must be manually inserted at any point that
    /// mutual recursion occurs.
    #[inline(always)]
    fn checked_recur<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&Self) -> Result<T, E>,
        E: From<RecursionLimitError>,
    {
        self.recursion_guard().descend()?;
        let out = maybe_grow(|| f(self));
        self.recursion_guard().ascend();
        out
    }

    /// Like [`CheckedRecursion::checked_recur`], but operates on a mutable
    /// reference to `Self`.
    #[inline(always)]
    fn checked_recur_mut<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<RecursionLimitError>,
    {
        self.recursion_guard().descend()?;
        let out = maybe_grow(|| f(self));
        self.recursion_guard().ascend();
        out
    }
}

/// Tracks recursion depth.
///
/// See the [`CheckedRecursion`] trait for usage instructions.
#[derive(Default, Debug, Clone)]
pub struct RecursionGuard {
    depth: RefCell<usize>,
    limit: usize,
}

impl CheckedRecursion for RecursionGuard {
    fn recursion_guard(&self) -> &RecursionGuard {
        self
    }
}

impl RecursionGuard {
    /// Constructs a new recursion guard with the specified recursion
    /// limit.
    pub fn with_limit(limit: usize) -> RecursionGuard {
        RecursionGuard {
            depth: RefCell::new(0),
            limit,
        }
    }

    fn descend(&self) -> Result<(), RecursionLimitError> {
        let mut depth = self.depth.borrow_mut();
        if *depth < self.limit {
            *depth += 1;
            Ok(())
        } else {
            Err(RecursionLimitError::new(self.limit))
        }
    }

    fn ascend(&self) {
        *self.depth.borrow_mut() -= 1;
    }
}

/// A [`RecursionGuard`]'s recursion limit was reached.
#[derive(Debug, Clone)]
pub struct RecursionLimitError {
    limit: usize,
    id: Option<u64>,
}

impl RecursionLimitError {
    /// Emits a backtrace of the too-deep recursion to logs
    /// and produces
    pub fn new(limit: usize) -> Self {
        #[cfg(feature = "tracing")]
        let id = {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);

            let backtrace = Backtrace::force_capture().to_string();
            error!(backtrace = %backtrace, "recursion limit error (backtrace id #{id})");

            Some(id)
        };
        #[cfg(not(feature = "tracing"))]
        let id = None;

        RecursionLimitError { limit, id }
    }
}

impl fmt::Display for RecursionLimitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let RecursionLimitError { limit, id } = self;
        write!(f, "exceeded recursion limit of {limit}",)?;
        if let Some(id) = id {
            write!(f, " (backtrace id #{id} in logs)")?;
        }
        writeln!(f)
    }
}

impl Error for RecursionLimitError {}

/// A drop-in replacement for `Box<T>` whose derivable trait implementations
/// are stack-safe.
///
/// Recursive tree types such as IR expressions must use `CheckedBox` (and
/// [`CheckedVec`]) instead of plain `Box`/`Vec` for their recursive fields.
/// The derived implementations of `Clone`, `PartialEq`, `Hash`, `Debug`,
/// `Serialize`, `Deserialize`, and the compiler-generated drop glue recurse
/// over the tree, and on a deep tree a plain `Box` or `Vec` edge overflows
/// the stack. `CheckedBox` delegates each of these operations to the wrapped
/// value inside [`maybe_grow`], so every recursion level crossing a
/// `CheckedBox` edge gets a stack-space check. A recursive field declared as
/// a plain `Box` or `Vec` silently reopens the overflow hole, so deep-tree
/// types should carry a regression test that exercises these operations on a
/// deep instance.
///
/// The wrapper behaves exactly like `Box<T>` for equality, ordering,
/// hashing, debug formatting, and serde serialization, so switching a field
/// between `Box<T>` and `CheckedBox<T>` never changes observable behavior,
/// only stack usage.
#[repr(transparent)]
pub struct CheckedBox<T>(ManuallyDrop<Box<T>>);

impl<T> CheckedBox<T> {
    /// Allocates a new `CheckedBox` holding `value`.
    pub fn new(value: T) -> Self {
        CheckedBox(ManuallyDrop::new(Box::new(value)))
    }

    /// Returns the wrapped value, consuming the box.
    pub fn into_inner(self) -> T {
        *self.into_box()
    }

    /// Returns the wrapped allocation as a plain `Box`.
    ///
    /// The caller becomes responsible for stack-safe teardown of the result.
    pub fn into_box(self) -> Box<T> {
        // Disarm our Drop impl, then move the box out. The inner value is
        // taken exactly once because `this` is never dropped.
        let mut this = ManuallyDrop::new(self);
        unsafe { ManuallyDrop::take(&mut this.0) }
    }
}

impl<T> From<T> for CheckedBox<T> {
    fn from(value: T) -> Self {
        CheckedBox::new(value)
    }
}

impl<T> From<Box<T>> for CheckedBox<T> {
    fn from(boxed: Box<T>) -> Self {
        CheckedBox(ManuallyDrop::new(boxed))
    }
}

impl<T> AsRef<T> for CheckedBox<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for CheckedBox<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

impl<T> Deref for CheckedBox<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for CheckedBox<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Default> Default for CheckedBox<T> {
    fn default() -> Self {
        CheckedBox::new(T::default())
    }
}

/// A drop-in replacement for `Vec<T>` whose derivable trait implementations
/// are stack-safe.
///
/// The companion of [`CheckedBox`] for recursive fields that hold a sequence
/// of children. See the `CheckedBox` documentation for the convention that
/// recursive fields of deep-tree types must use these wrappers.
///
/// Elements are stored inline in the wrapped `Vec`, so there is no
/// per-element indirection or allocation, and the memory layout matches a
/// plain `Vec<T>`. The stack-safety guard wraps each whole per-element
/// operation loop once. This suffices as long as every recursion cycle in
/// the wrapped type crosses at least one `CheckedBox` or `CheckedVec` edge,
/// and the bounded number of unguarded hops between two guarded edges fits
/// in the [`STACK_RED_ZONE`].
///
/// `Deref`/`DerefMut` to `Vec<T>` hands back the entire `Vec` API, and the
/// wrapper behaves exactly like `Vec<T>` for equality, ordering, hashing,
/// debug formatting, and serde serialization.
#[repr(transparent)]
pub struct CheckedVec<T>(ManuallyDrop<Vec<T>>);

impl<T> CheckedVec<T> {
    /// Creates a new empty `CheckedVec`.
    pub fn new() -> Self {
        CheckedVec(ManuallyDrop::new(Vec::new()))
    }

    /// Returns the wrapped `Vec`, consuming the wrapper.
    ///
    /// The caller becomes responsible for stack-safe teardown of the result.
    pub fn into_inner(self) -> Vec<T> {
        // Disarm our Drop impl, then move the vec out. The inner value is
        // taken exactly once because `this` is never dropped.
        let mut this = ManuallyDrop::new(self);
        unsafe { ManuallyDrop::take(&mut this.0) }
    }
}

impl<T> From<Vec<T>> for CheckedVec<T> {
    fn from(vec: Vec<T>) -> Self {
        CheckedVec(ManuallyDrop::new(vec))
    }
}

impl<T> FromIterator<T> for CheckedVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from(Vec::from_iter(iter))
    }
}

impl<T> IntoIterator for CheckedVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_inner().into_iter()
    }
}

impl<'a, T> IntoIterator for &'a CheckedVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut CheckedVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T> Extend<T> for CheckedVec<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        (**self).extend(iter)
    }
}

impl<T> Deref for CheckedVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Vec<T> {
        &self.0
    }
}

impl<T> DerefMut for CheckedVec<T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.0
    }
}

impl<T> Default for CheckedVec<T> {
    fn default() -> Self {
        CheckedVec::new()
    }
}

/// Generates the stack-safe trait implementations shared by [`CheckedBox`]
/// and [`CheckedVec`].
///
/// Every generated impl delegates the operation to the wrapped value (the
/// wrapper's `Deref` target) inside [`maybe_grow`]. Generating both sets
/// from one list keeps the safety-critical set of guarded operations in
/// sync between the two wrappers.
macro_rules! checked_impls {
    ($wrapper:ident, $target:ty) => {
        impl<T> Clone for $wrapper<T>
        where
            $target: Clone,
        {
            fn clone(&self) -> Self {
                maybe_grow(|| Self::from((**self).clone()))
            }
        }

        impl<T> PartialEq for $wrapper<T>
        where
            $target: PartialEq,
        {
            fn eq(&self, other: &Self) -> bool {
                maybe_grow(|| **self == **other)
            }
        }

        impl<T> Eq for $wrapper<T> where $target: Eq {}

        impl<T> PartialOrd for $wrapper<T>
        where
            $target: PartialOrd,
        {
            fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
                maybe_grow(|| (**self).partial_cmp(other))
            }
        }

        impl<T> Ord for $wrapper<T>
        where
            $target: Ord,
        {
            fn cmp(&self, other: &Self) -> CmpOrdering {
                maybe_grow(|| (**self).cmp(other))
            }
        }

        impl<T> Hash for $wrapper<T>
        where
            $target: Hash,
        {
            fn hash<H: Hasher>(&self, state: &mut H) {
                maybe_grow(|| (**self).hash(state))
            }
        }

        impl<T> fmt::Debug for $wrapper<T>
        where
            $target: fmt::Debug,
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                maybe_grow(|| (**self).fmt(f))
            }
        }

        // NOTE: the CheckedVec instantiation of this impl is inert, because
        // Vec<T> never implements Display. It is generated anyway to keep
        // the guarded operation list identical for both wrappers.
        impl<T> fmt::Display for $wrapper<T>
        where
            $target: fmt::Display,
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                maybe_grow(|| (**self).fmt(f))
            }
        }

        impl<T> Serialize for $wrapper<T>
        where
            $target: Serialize,
        {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                maybe_grow(|| (**self).serialize(serializer))
            }
        }

        impl<'de, T> Deserialize<'de> for $wrapper<T>
        where
            $target: Deserialize<'de>,
        {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                maybe_grow(|| <$target>::deserialize(deserializer).map(Self::from))
            }
        }

        impl<T> Drop for $wrapper<T> {
            fn drop(&mut self) {
                // ManuallyDrop lets us run the inner teardown inside
                // maybe_grow. Plain drop glue would recurse before we get a
                // chance to check the remaining stack space.
                maybe_grow(|| unsafe { ManuallyDrop::drop(&mut self.0) })
            }
        }
    };
}

checked_impls!(CheckedBox, T);
checked_impls!(CheckedVec, Vec<T>);

/// Test support: runs `f` on a freshly spawned thread with a stack of
/// `stack_bytes` bytes and returns the result of joining it, so a panic in
/// `f` is propagated to the caller as an `Err`.
///
/// Deep-tree regression tests use this to prove that an operation is
/// stack-safe: the operation must complete on a stack much smaller than the
/// tree is deep.
pub fn run_on_small_stack<F>(stack_bytes: usize, f: F) -> std::thread::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    std::thread::Builder::new()
        .name("small-stack".into())
        .stack_size(stack_bytes)
        .spawn(f)
        .expect("thread spawns")
        .join()
}
