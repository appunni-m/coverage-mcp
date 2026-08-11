//! Bounded DuckDB pooling and interruptible operation deadlines.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use duckdb::{Connection, DuckdbConnectionManager, InterruptHandle};

use crate::error::{AppError, AppResult};

/// DuckDB pool type backed by the maintained duckdb-rs r2d2 manager.
pub(crate) type DbPool = r2d2::Pool<DuckdbConnectionManager>;
/// One leased connection from a project pool.
pub(crate) type DbConnection = r2d2::PooledConnection<DuckdbConnectionManager>;
type WatchdogSpawner = fn(
    Arc<(Mutex<bool>, Condvar)>,
    Arc<AtomicBool>,
    Arc<InterruptHandle>,
    Duration,
) -> AppResult<thread::JoinHandle<()>>;

/// Builds a bounded pool that clones connections from one DuckDB database handle.
pub(crate) fn open_pool(path: &std::path::Path, max_size: usize) -> AppResult<DbPool> {
    let manager = DuckdbConnectionManager::file(path)?;
    r2d2::Pool::builder()
        .max_size(max_size as u32)
        .test_on_check_out(true)
        .build(manager)
        .map_err(AppError::from)
}

/// Checks out a connection with a bounded wait instead of blocking forever.
#[rustfmt::skip]
pub(crate) fn checkout(
    pool: &DbPool,
    timeout: Duration,
    resource: &str) -> AppResult<DbConnection> {
    pool.get_timeout(timeout).map_err(|error| AppError::Busy {
        resource: format!("DuckDB connection for {resource}"),
        holder: format!(" (pool checkout failed: {error})"),
    })
}

/// Tracks leased connections so shutdown can interrupt active work first.
#[derive(Default)]
pub(crate) struct QueryTracker {
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, Arc<InterruptHandle>>>,
    idle: Condvar,
}

impl QueryTracker {
    pub(crate) fn begin(&self, interrupt: Arc<InterruptHandle>) -> AppResult<QueryGuard<'_>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active
            .lock()
            .map_err(|_| AppError::Runtime("DuckDB query tracker lock poisoned".to_owned()))?
            .insert(id, interrupt);
        Ok(QueryGuard { tracker: self, id })
    }

    pub(crate) fn interrupt_all(&self) {
        let handles = self
            .active
            .lock()
            .map(|active| active.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for handle in handles {
            handle.interrupt();
        }
    }

    #[rustfmt::skip]
    pub(crate) fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let Ok(mut active) = self.active.lock() else {
            return false;
        };
        while !active.is_empty() {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let Ok((next, wait)) = self.idle.wait_timeout(active, remaining) else {
                return false;
            };
            active = next;
            if wait.timed_out() || Instant::now() >= deadline { return false; }
        }
        true
    }

    fn finish(&self, id: u64) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&id);
            if active.is_empty() {
                self.idle.notify_all();
            }
        }
    }
}

/// RAII registration for one active DuckDB operation.
pub(crate) struct QueryGuard<'a> {
    tracker: &'a QueryTracker,
    id: u64,
}

impl Drop for QueryGuard<'_> {
    fn drop(&mut self) {
        self.tracker.finish(self.id);
    }
}

/// Runs one operation with a watchdog that calls DuckDB's interrupt handle.
fn spawn_watchdog(
    done: Arc<(Mutex<bool>, Condvar)>,
    timed_out: Arc<AtomicBool>,
    interrupt: Arc<InterruptHandle>,
    timeout: Duration,
) -> AppResult<thread::JoinHandle<()>> {
    let watchdog_done = Arc::clone(&done);
    let watchdog_timed_out = Arc::clone(&timed_out);
    thread::Builder::new()
        .name("coverage-mcp-db-watchdog".to_owned())
        .spawn(move || {
            let deadline = Instant::now() + timeout;
            let (done_lock, done_cv) = &*watchdog_done;
            let Ok(mut complete) = done_lock.lock() else {
                watchdog_timed_out.store(true, Ordering::SeqCst);
                interrupt.interrupt();
                return;
            };
            loop {
                if *complete {
                    return;
                }
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    watchdog_timed_out.store(true, Ordering::SeqCst);
                    interrupt.interrupt();
                    return;
                };
                let Ok((next, wait)) = done_cv.wait_timeout(complete, remaining) else {
                    watchdog_timed_out.store(true, Ordering::SeqCst);
                    interrupt.interrupt();
                    return;
                };
                complete = next;
                if wait.timed_out() && !*complete {
                    watchdog_timed_out.store(true, Ordering::SeqCst);
                    interrupt.interrupt();
                    return;
                }
            }
        })
        .map_err(watchdog_spawn_error)
}

fn watchdog_spawn_error(error: std::io::Error) -> AppError {
    AppError::Runtime(format!("could not start DuckDB watchdog: {error}"))
}

#[rustfmt::skip]
pub(crate) fn run_with_timeout<T>(
    connection: &Connection,
    timeout: Duration,
    operation: &str,
    function: impl FnOnce(&Connection) -> AppResult<T>) -> AppResult<T> {
    run_with_timeout_using(connection, timeout, operation, function, spawn_watchdog)
}

#[rustfmt::skip]
fn run_with_timeout_using<T>(
    connection: &Connection,
    timeout: Duration,
    operation: &str,
    function: impl FnOnce(&Connection) -> AppResult<T>,
    spawn: WatchdogSpawner) -> AppResult<T> {
    let done = Arc::new((Mutex::new(false), Condvar::new()));
    let timed_out = Arc::new(AtomicBool::new(false));
    let watchdog = spawn(
        Arc::clone(&done),
        Arc::clone(&timed_out),
        connection.interrupt_handle(), timeout)?;
    let result = function(connection);
    mark_complete(&done);
    let _ = watchdog.join();
    timeout_error_if_set(timed_out.load(Ordering::SeqCst), operation, timeout).map_or(result, Err)
}

#[rustfmt::skip]
fn timeout_error(operation: &str, timeout: Duration) -> AppError {
    AppError::Timeout { operation: operation.to_owned(), timeout_ms: timeout.as_millis() as u64 }
}

fn timeout_error_if_set(timed_out: bool, operation: &str, timeout: Duration) -> Option<AppError> {
    timed_out.then(|| timeout_error(operation, timeout))
}

fn mark_complete(done: &(Mutex<bool>, Condvar)) {
    if let Ok(mut complete) = done.0.lock() {
        *complete = true;
        done.1.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_checkout_times_out_instead_of_waiting_forever() {
        let directory = tempfile::tempdir().expect("tempdir");
        let pool = open_pool(&directory.path().join("pool.duckdb"), 1).expect("pool");
        let first = checkout(&pool, Duration::from_millis(50), "test").expect("first");
        assert!(matches!(
            checkout(&pool, Duration::from_millis(5), "test"),
            Err(AppError::Busy { .. })
        ));
        drop(first);
        assert!(checkout(&pool, Duration::from_millis(50), "test").is_ok());
    }

    #[test]
    fn operation_watchdog_returns_a_typed_timeout() {
        let connection = Connection::open_in_memory().expect("connection");
        let error = run_with_timeout(
            &connection,
            Duration::from_millis(5),
            "test operation",
            |_| {
                thread::sleep(Duration::from_millis(20));
                Ok(())
            },
        )
        .expect_err("timeout");
        assert!(matches!(error, AppError::Timeout { .. }));
    }

    #[test]
    fn query_tracker_interrupts_and_waits_for_active_connections() {
        let tracker = QueryTracker::default();
        let connection = Connection::open_in_memory().expect("connection");
        let guard = tracker
            .begin(connection.interrupt_handle())
            .expect("query guard");
        tracker.interrupt_all();
        assert!(!tracker.wait_for_idle(Duration::from_millis(1)));
        drop(guard);
        assert!(tracker.wait_for_idle(Duration::from_millis(10)));
    }

    #[test]
    fn query_tracker_deadlines_and_poisoned_waits_fail_closed() {
        let tracker = Arc::new(QueryTracker::default());
        let connection = Connection::open_in_memory().expect("connection");
        let guard = tracker
            .begin(connection.interrupt_handle())
            .expect("query guard");
        assert!(!tracker.wait_for_idle(Duration::ZERO));
        assert!(!tracker.wait_for_idle(Duration::from_millis(20)));
        assert!(!tracker.wait_for_idle(Duration::from_secs(1)));
        drop(guard);

        let releasing_tracker = Arc::new(QueryTracker::default());
        let ready = Arc::new(std::sync::Barrier::new(2));
        let releasing_thread_tracker = Arc::clone(&releasing_tracker);
        let releasing_thread_ready = Arc::clone(&ready);
        let releasing_thread = thread::spawn(move || {
            let connection = Connection::open_in_memory().expect("connection");
            let _guard = releasing_thread_tracker
                .begin(connection.interrupt_handle())
                .expect("query guard");
            releasing_thread_ready.wait();
            thread::sleep(Duration::from_millis(20));
        });
        ready.wait();
        assert!(releasing_tracker.wait_for_idle(Duration::from_secs(1)));
        releasing_thread.join().expect("releasing thread");

        let poisoned = Arc::new(QueryTracker::default());
        let connection = Connection::open_in_memory().expect("connection");
        let guard = poisoned
            .begin(connection.interrupt_handle())
            .expect("query guard");
        let poison_target = Arc::clone(&poisoned);
        let poisoner = thread::spawn(move || {
            let _lock = poison_target.active.lock().expect("active lock");
            panic!("injected active tracker poison");
        });
        assert!(poisoner.join().is_err());
        assert!(matches!(
            poisoned.begin(connection.interrupt_handle()),
            Err(AppError::Runtime(_))
        ));
        assert!(!poisoned.wait_for_idle(Duration::from_millis(1)));
        poisoned.interrupt_all();
        drop(guard);

        let condvar_poisoned = Arc::new(QueryTracker::default());
        let connection = Connection::open_in_memory().expect("connection");
        let guard = condvar_poisoned
            .begin(connection.interrupt_handle())
            .expect("query guard");
        let poison_target = Arc::clone(&condvar_poisoned);
        let poisoner = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let _lock = poison_target.active.lock().expect("active lock");
            panic!("injected condvar wait poison");
        });
        assert!(!condvar_poisoned.wait_for_idle(Duration::from_millis(200)));
        assert!(poisoner.join().is_err());
        drop(guard);
    }

    #[test]
    fn watchdog_poison_deadline_and_wait_errors_interrupt_safely() {
        fn successful_operation(_: &Connection) -> AppResult<()> {
            Ok(())
        }

        #[rustfmt::skip]
        fn fail_to_spawn_watchdog(_: Arc<(Mutex<bool>, Condvar)>, _: Arc<AtomicBool>, _: Arc<InterruptHandle>, _: Duration) -> AppResult<thread::JoinHandle<()>> {
            Err(AppError::Runtime("injected watchdog creation failure".to_owned()))
        }

        let connection = Connection::open_in_memory().expect("connection");

        let completed = Arc::new((Mutex::new(false), Condvar::new()));
        mark_complete(&completed);
        assert!(*completed.0.lock().expect("completed lock"));
        let poisoned_complete = Arc::new((Mutex::new(false), Condvar::new()));
        let poison_target = Arc::clone(&poisoned_complete);
        let poisoner = thread::spawn(move || {
            let _lock = poison_target.0.lock().expect("completed lock");
            panic!("injected completed lock poison");
        });
        assert!(poisoner.join().is_err());
        mark_complete(&poisoned_complete);

        let poisoned_done = Arc::new((Mutex::new(false), Condvar::new()));
        let poison_target = Arc::clone(&poisoned_done);
        let poisoner = thread::spawn(move || {
            let _lock = poison_target.0.lock().expect("done lock");
            panic!("injected watchdog lock poison");
        });
        assert!(poisoner.join().is_err());
        let poisoned_timeout = Arc::new(AtomicBool::new(false));
        let watchdog = spawn_watchdog(
            poisoned_done,
            Arc::clone(&poisoned_timeout),
            connection.interrupt_handle(),
            Duration::from_secs(1),
        )
        .expect("watchdog");
        watchdog.join().expect("watchdog join");
        assert!(poisoned_timeout.load(Ordering::SeqCst));

        let deadline_done = Arc::new((Mutex::new(false), Condvar::new()));
        let deadline_timeout = Arc::new(AtomicBool::new(false));
        let watchdog = spawn_watchdog(
            deadline_done,
            Arc::clone(&deadline_timeout),
            connection.interrupt_handle(),
            Duration::ZERO,
        )
        .expect("watchdog");
        watchdog.join().expect("watchdog join");
        assert!(deadline_timeout.load(Ordering::SeqCst));

        let wait_done = Arc::new((Mutex::new(false), Condvar::new()));
        let wait_timeout = Arc::new(AtomicBool::new(false));
        let poison_target = Arc::clone(&wait_done);
        let watchdog = spawn_watchdog(
            Arc::clone(&wait_done),
            Arc::clone(&wait_timeout),
            connection.interrupt_handle(),
            Duration::from_millis(200),
        )
        .expect("watchdog");
        thread::sleep(Duration::from_millis(10));
        let poisoner = thread::spawn(move || {
            let _lock = poison_target.0.lock().expect("done lock");
            panic!("injected watchdog wait poison");
        });
        assert!(poisoner.join().is_err());
        watchdog.join().expect("watchdog join");
        assert!(wait_timeout.load(Ordering::SeqCst));

        assert!(successful_operation(&connection).is_ok());
        let error = run_with_timeout_using(
            &connection,
            Duration::from_secs(1),
            "watchdog creation",
            successful_operation,
            fail_to_spawn_watchdog,
        )
        .expect_err("watchdog creation failure");
        assert!(matches!(error, AppError::Runtime(_)));

        let result = run_with_timeout(
            &connection,
            Duration::from_secs(1),
            "successful operation",
            |_| Ok(42_u8),
        )
        .expect("successful operation");
        assert_eq!(result, 42);
        assert_eq!(
            watchdog_spawn_error(std::io::Error::other("injected watchdog error")).to_string(),
            "could not start DuckDB watchdog: injected watchdog error"
        );
    }
}
