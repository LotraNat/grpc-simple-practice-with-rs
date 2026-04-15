//! Shared test harness for integration & concurrency suites.
//!
//! NOTE: If you see `test_echo` / `test_example` / `test_kv_store` failing,
//! those are the autograder's tests in src/test.rs — they expect an *external*
//! server at 127.0.0.1:10162. The tests in tests/ spin up their own in-process
//! server and will panic if they detect an external one running.
//!
//! Run new tests with no external server:
//!     pkill -f target/debug/server
//!     cargo test --test integration
//!     cargo test --test concurrency -- --nocapture
//!
//! Implementation note: each `#[tokio::test]` creates its own tokio runtime
//! that is dropped when the test ends. If we spawned the server on the first
//! test's runtime via `tokio::spawn`, the server would die when that test
//! finished. Instead, we run the server on a *dedicated OS thread* with its
//! own long-lived tokio runtime, created once via `std::sync::Once`.

#![allow(dead_code)]

use lab_grpc::{server, SERVER_ADDR};
use std::net::TcpStream as StdTcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};

static INIT: Once = Once::new();
static SERVER_UP: AtomicBool = AtomicBool::new(false);

/// Ensure an in-process server is running on SERVER_ADDR. Idempotent, safe
/// to call concurrently.
pub async fn ensure_server() {
    INIT.call_once(|| {
        // Pre-flight: detect an already-listening external server.
        if StdTcpStream::connect_timeout(
            &SERVER_ADDR.parse().expect("SERVER_ADDR must parse"),
            Duration::from_millis(100),
        )
        .is_ok()
        {
            panic!(
                "An external server is already listening on {}. Kill it before \
                running these tests (e.g., `pkill -f target/debug/server`).",
                SERVER_ADDR
            );
        }

        // Spawn a dedicated OS thread with its own tokio runtime so the server
        // outlives any individual `#[tokio::test]`'s runtime.
        thread::Builder::new()
            .name("test-grpc-server".into())
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime for test server");
                rt.block_on(async {
                    if let Err(e) = server::start_on(SERVER_ADDR).await {
                        eprintln!("server::start_on exited with error: {:?}", e);
                    }
                });
            })
            .expect("failed to spawn test server thread");

        // Block until the socket accepts connections. Use std (blocking) TCP
        // because we're inside a sync closure (not async).
        let deadline = Duration::from_secs(5);
        let start = Instant::now();
        let addr: std::net::SocketAddr = SERVER_ADDR.parse().unwrap();
        loop {
            if StdTcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
                break;
            }
            if start.elapsed() > deadline {
                panic!(
                    "server failed to come up on {} within {:?}",
                    SERVER_ADDR, deadline
                );
            }
            thread::sleep(Duration::from_millis(25));
        }

        SERVER_UP.store(true, Ordering::Release);
    });

    // Callers that didn't run the `Once` closure still need to see the flag.
    // By the time `call_once` returns to any caller, the starter has finished
    // its closure, so SERVER_UP is already true.
    debug_assert!(SERVER_UP.load(Ordering::Acquire));
}

/// Namespace a test's keys so tests running in parallel on the shared server
/// don't collide.
pub fn unique_prefix(test_name: &str) -> Vec<u8> {
    format!("test::{}::", test_name).into_bytes()
}

/// Concatenate a prefix with a suffix.
pub fn k(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(prefix.len() + suffix.len());
    v.extend_from_slice(prefix);
    v.extend_from_slice(suffix);
    v
}

/// Random byte vector of length n (for stress tests).
pub fn random_bytes(n: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut v = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut v);
    v
}
