//! Methods for custom fork-join scopes, created by the [`external_scope()`]
//! function. These are like [`scope()`] but the scope operation runs
//! on the thread calling [`external_scope()`], not in the Rayon thread pool.
//!
//! [`external_scope()`]: fn.external_scope.html

use crate::job::HeapJob;
use crate::latch::{Latch, LockLatch};
use crate::registry::{Registry, WorkerThread};
use crate::unwind;
use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, AtomicPtr, Ordering};
use std::sync::Arc;

#[cfg(test)]
mod test;

/// Represents a fork-join scope which can be used to spawn any number of tasks.
/// See [`external_scope()`] for more information.
///
///[`external_scope()`]: fn.external_scope.html
pub struct ExternalScope<'scope> {
    base: ExternalScopeBase<'scope>,
}

struct ExternalScopeBase<'scope> {
    /// thread registry to spawn jobs in
    registry: Arc<Registry>,

    /// if some job panicked, the error is stored here; it will be
    /// propagated to the one who created the scope
    panic: AtomicPtr<Box<dyn Any + Send + 'static>>,

    /// Number of active jobs in the scope
    job_count: AtomicUsize,

    /// latch to set when the counter drops to zero (and hence this scope is complete)
    completion_latch: LockLatch,

    /// You can think of a scope as containing a list of closures to execute,
    /// all of which outlive `'scope`.  They're not actually required to be
    /// `Sync`, but it's still safe to let the `Scope` implement `Sync` because
    /// the closures are only *moved* across threads to be executed.
    marker: PhantomData<Box<dyn FnOnce(&ExternalScope<'scope>) + Send + Sync + 'scope>>,
}

/// Creates a "fork-join" scope `s` and invokes the closure with a
/// reference to `s`. This closure can then spawn asynchronous tasks
/// into `s`. Those tasks may run asynchronously with respect to the
/// closure; they may themselves spawn additional tasks into `s`. When
/// the closure returns, it will block until all tasks that have been
/// spawned into `s` complete.
///
/// This is just like `scope()` except the closure runs on the same thread
/// that calls `external_scope()`. Only work that it spawns runs in the
/// thread pool.
///
/// # Panics
///
/// If a panic occurs, either in the closure given to `scope()` or in
/// any of the spawned jobs, that panic will be propagated and the
/// call to `scope()` will panic. If multiple panics occurs, it is
/// non-deterministic which of their panic values will propagate.
/// Regardless, once a task is spawned using `scope.spawn()`, it will
/// execute, even if the spawning task should later panic. `scope()`
/// returns once all spawned jobs have completed, and any panics are
/// propagated at that point.
pub fn external_scope<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&ExternalScope<'scope>) -> R,
{
    let worker_thread = WorkerThread::current();
    let registry = if worker_thread.is_null() {
         Registry::current()
    } else {
         panic!("Can't use external_scope() from a worker thread");
    };
    ExternalScope::<'scope>::run(registry, op)
}

impl<'scope> ExternalScope<'scope> {
    pub(crate) fn run<OP, R>(registry: Arc<Registry>, op: OP) -> R
    where OP: FnOnce(&ExternalScope<'scope>) -> R
    {
        let scope = ExternalScope {
            base: ExternalScopeBase::new(registry),
        };
        let r = op(&scope);
        unsafe { scope.base.complete() };
        r
    }

    /// Spawns a job into the fork-join scope `self`. This job will
    /// execute sometime before the fork-join scope completes.  The
    /// job is specified as a closure, and this closure receives its
    /// own reference to the scope `self` as argument. This can be
    /// used to inject new jobs into `self`.
    ///
    /// # Returns
    ///
    /// Nothing. The spawned closures cannot pass back values to the
    /// caller directly, though they can write to local variables on
    /// the stack (if those variables outlive the scope) or
    /// communicate through shared channels.
    ///
    /// (The intention is to eventualy integrate with Rust futures to
    /// support spawns of functions that compute a value.)
    pub fn spawn<BODY>(&self, body: BODY)
    where
        BODY: FnOnce(&ExternalScope<'scope>) + Send + 'scope,
    {
        self.base.increment();
        unsafe {
            let job_ref = Box::new(HeapJob::new(move || {
                self.base.execute_job(move || body(self))
            }))
            .as_job_ref();

            self.base.registry.inject_or_push(job_ref);
        }
    }
}

impl<'scope> ExternalScopeBase<'scope> {
    /// Creates the base of a new scope for the given worker thread
    fn new(registry: Arc<Registry>) -> Self {
        ExternalScopeBase {
            registry: registry,
            panic: AtomicPtr::new(ptr::null_mut()),
            job_count: AtomicUsize::new(1),
            completion_latch: LockLatch::new(),
            marker: PhantomData,
        }
    }

    fn increment(&self) {
        self.job_count.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn complete(&self)
    {
        self.job_completed();
        // wait for job counter to reach 0
        self.completion_latch.wait();

        // propagate panic, if any occurred; at this point, all
        // outstanding jobs have completed, so we can use a relaxed
        // ordering:
        let panic = self.panic.swap(ptr::null_mut(), Ordering::Relaxed);
        if !panic.is_null() {
            let value: Box<Box<dyn Any + Send + 'static>> = mem::transmute(panic);
            unwind::resume_unwinding(*value);
        }
    }

    /// Executes `func` as a job in scope. Adjusts the "job completed"
    /// counters and also catches any panic and stores it into
    /// `scope`.
    unsafe fn execute_job<FUNC>(&self, func: FUNC)
    where
        FUNC: FnOnce(),
    {
        if let Err(err) = unwind::halt_unwinding(func) {
            self.job_panicked(err);
        }
        self.job_completed();
    }

    unsafe fn job_panicked(&self, err: Box<dyn Any + Send + 'static>) {
        // capture the first error we see, free the rest
        let nil = ptr::null_mut();
        let mut err = Box::new(err); // box up the fat ptr
        if self
            .panic
            .compare_exchange(nil, &mut *err, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            mem::forget(err); // ownership now transferred into self.panic
        }
    }

    unsafe fn job_completed(&self) {
        if self.job_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.completion_latch.set();
        }
    }
}

impl<'scope> fmt::Debug for ExternalScope<'scope> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ExternalScope")
            .field("pool_id", &self.base.registry.id())
            .field("panic", &self.base.panic)
            .field("job_count", &self.base.job_count)
            .finish()
    }
}

