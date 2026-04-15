//! Concurrency / stress tests — the main multithread suite.
//!
//! Every test wraps its body in a 30s timeout so a deadlock surfaces as a
//! clear failure rather than a hung runner.
//!
//! Run with `cargo test --test concurrency -- --nocapture` to see per-test
//! progress and timing output.

mod common;

use common::{ensure_server, k, unique_prefix};
use futures_util::future::join_all;
use lab_grpc::client::{get, put};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::timeout;

async fn join_handles<T: Send + 'static>(
    handles: Vec<tokio::task::JoinHandle<T>>,
) -> Vec<T> {
    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn hang_msg() -> &'static str {
    "test hung — likely a deadlock in your RwLock usage"
}

// C1
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_parallel_puts_distinct_keys() {
    ensure_server().await;
    let p = Arc::new(unique_prefix("par_puts"));
    eprintln!("[C1] 100 parallel puts on distinct keys, then verify all 100");

    timeout(TEST_TIMEOUT, async {
        let started = Instant::now();
        let mut handles = Vec::new();
        for i in 0u32..100 {
            let p = Arc::clone(&p);
            handles.push(tokio::spawn(async move {
                let key = k(&p, format!("{}", i).as_bytes());
                let val = format!("val-{}", i).into_bytes();
                put(key, val).await.expect("put failed")
            }));
        }
        join_handles(handles).await;
        eprintln!("[C1]   100 parallel puts finished in {:?}", started.elapsed());

        for i in 0u32..100 {
            let key = k(&p, format!("{}", i).as_bytes());
            let got = get(key).await.unwrap_or_else(|e| panic!("get failed at i={}: {:?}", i, e));
            assert_eq!(got, format!("val-{}", i).into_bytes(), "mismatch at i={}", i);
        }
        eprintln!("[C1]   all 100 values verified ✓");
    })
    .await
    .expect(hang_msg());
}

// C2
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_parallel_reads_same_key() {
    ensure_server().await;
    let p = unique_prefix("par_reads");
    let key = k(&p, b"hot");
    let value = b"shared-value".to_vec();
    eprintln!("[C2] 200 parallel gets on a single hot key");

    put(key.clone(), value.clone()).await.unwrap();

    timeout(TEST_TIMEOUT, async {
        let started = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..200 {
            let key = key.clone();
            handles.push(tokio::spawn(async move { get(key).await }));
        }
        let results = join_handles(handles).await;
        for r in &results {
            let v = r.as_ref().expect("get returned Err");
            assert_eq!(v, &value);
        }
        assert_eq!(results.len(), 200);
        eprintln!(
            "[C2]   200 concurrent gets completed in {:?} ✓ \
             (if this is more than a few seconds, reads may be serialized — Mutex instead of RwLock?)",
            started.elapsed()
        );
    })
    .await
    .expect(hang_msg());
}

// C3
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_readers_and_writers_mixed() {
    ensure_server().await;
    let p = Arc::new(unique_prefix("mixed_rw"));
    eprintln!("[C3] 50 readers × 20 gets + 20 writers × 5 puts over 20 shared keys");

    // Pre-populate 20 keys with a known value.
    for i in 0u32..20 {
        put(
            k(&p, format!("{}", i).as_bytes()),
            format!("init-{}", i).into_bytes(),
        )
        .await
        .unwrap();
    }
    eprintln!("[C3]   pre-populated 20 keys");

    timeout(TEST_TIMEOUT, async {
        let started = Instant::now();
        let mut handles = Vec::new();

        // 50 reader tasks, each 20 random-ish gets.
        for r in 0..50u32 {
            let p = Arc::clone(&p);
            handles.push(tokio::spawn(async move {
                for j in 0u32..20 {
                    let i = (r * 31 + j) % 20;
                    let _ = get(k(&p, format!("{}", i).as_bytes())).await;
                }
            }));
        }

        // 20 writer tasks, each 5 puts updating existing keys.
        for w in 0..20u32 {
            let p = Arc::clone(&p);
            handles.push(tokio::spawn(async move {
                for j in 0u32..5 {
                    let i = (w * 7 + j) % 20;
                    let val = format!("w{}-j{}", w, j).into_bytes();
                    put(k(&p, format!("{}", i).as_bytes()), val)
                        .await
                        .expect("put failed");
                }
            }));
        }

        join_handles(handles).await;
        eprintln!("[C3]   mixed workload finished in {:?}", started.elapsed());

        // Final state: every key must be readable as *some* Ok value.
        for i in 0u32..20 {
            let res = get(k(&p, format!("{}", i).as_bytes())).await;
            assert!(res.is_ok(), "key {} became unreadable after mixed load", i);
        }
        eprintln!("[C3]   all 20 keys still readable ✓");
    })
    .await
    .expect(hang_msg());
}

// C4
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_racing_puts_same_key() {
    ensure_server().await;
    let p = unique_prefix("racing_puts");
    let key = k(&p, b"contested");
    eprintln!("[C4] 50 puts racing on one key — final value must equal one of them");

    let submitted: HashSet<Vec<u8>> = (0..50u32)
        .map(|i| format!("racer-{}", i).into_bytes())
        .collect();

    timeout(TEST_TIMEOUT, async {
        let mut handles = Vec::new();
        for v in &submitted {
            let key = key.clone();
            let v = v.clone();
            handles.push(tokio::spawn(async move { put(key, v).await }));
        }
        for r in join_handles(handles).await {
            r.expect("put returned Err");
        }

        let final_val = get(key).await.expect("final get returned Err");
        assert!(
            submitted.contains(&final_val),
            "final value {:?} is not in the submitted set — corrupted write?",
            final_val
        );
        eprintln!(
            "[C4]   final winner = {:?} (one of {} racers) ✓",
            String::from_utf8_lossy(&final_val),
            submitted.len()
        );
    })
    .await
    .expect(hang_msg());
}

// C5
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_put_then_get_race() {
    ensure_server().await;
    let p = Arc::new(unique_prefix("raw_race"));
    eprintln!("[C5] 200 tasks each doing put-then-get on its own key");

    timeout(TEST_TIMEOUT, async {
        let started = Instant::now();
        let mut handles = Vec::new();
        for i in 0u32..200 {
            let p = Arc::clone(&p);
            handles.push(tokio::spawn(async move {
                let key = k(&p, format!("{}", i).as_bytes());
                let val = format!("v{}", i).into_bytes();
                put(key.clone(), val.clone())
                    .await
                    .unwrap_or_else(|e| panic!("put failed at i={}: {:?}", i, e));
                let got = get(key)
                    .await
                    .unwrap_or_else(|e| panic!("get failed at i={}: {:?}", i, e));
                assert_eq!(
                    got, val,
                    "read-after-write mismatch at i={} — write not visible to subsequent get",
                    i
                );
            }));
        }
        join_handles(handles).await;
        eprintln!("[C5]   200 read-after-write pairs verified in {:?} ✓", started.elapsed());
    })
    .await
    .expect(hang_msg());
}

// C6
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_stress_1000_mixed_ops() {
    ensure_server().await;
    let p = Arc::new(unique_prefix("stress"));
    eprintln!("[C6] stress: 50 tasks × 20 ops (~70% put, 30% get) over 30 keys");

    // Oracle: per-key history of every value ever written.
    // A final Get's value must be a member of the corresponding history.
    let history: Arc<Mutex<HashMap<Vec<u8>, HashSet<Vec<u8>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Pre-populate all 30 keys so Gets don't race with the first Put.
    for i in 0u32..30 {
        let key = k(&p, format!("{}", i).as_bytes());
        let val = format!("seed-{}", i).into_bytes();
        put(key.clone(), val.clone()).await.unwrap();
        history
            .lock()
            .await
            .entry(key)
            .or_insert_with(HashSet::new)
            .insert(val);
    }
    eprintln!("[C6]   seeded 30 keys");

    timeout(TEST_TIMEOUT, async {
        let started = Instant::now();
        let mut handles = Vec::new();
        for t in 0u32..50 {
            let p = Arc::clone(&p);
            let history = Arc::clone(&history);
            handles.push(tokio::spawn(async move {
                use rand::{rngs::StdRng, Rng, SeedableRng};
                let mut rng = StdRng::seed_from_u64(t as u64);

                for op in 0u32..20 {
                    let i = rng.gen_range(0..30u32);
                    let key = k(&p, format!("{}", i).as_bytes());

                    if rng.gen_range(0..10) < 7 {
                        // 70% puts
                        let val = format!("t{}-op{}-r{}", t, op, rng.gen::<u16>()).into_bytes();
                        {
                            let mut h = history.lock().await;
                            h.entry(key.clone()).or_insert_with(HashSet::new).insert(val.clone());
                        }
                        put(key, val).await.expect("put failed");
                    } else {
                        // 30% gets — must Ok (every key pre-populated)
                        let _ = get(key).await.expect("get on pre-populated key returned Err");
                    }
                }
            }));
        }
        join_handles(handles).await;
        eprintln!("[C6]   1000 mixed ops completed in {:?}", started.elapsed());

        // Verify final state: each key's value must be in its history set.
        let h = history.lock().await;
        for (key, values) in h.iter() {
            let got = get(key.clone())
                .await
                .unwrap_or_else(|e| panic!("final get failed on {:?}: {:?}", key, e));
            assert!(
                values.contains(&got),
                "final value {:?} for key {:?} is not in the recorded history",
                got,
                key
            );
        }
        eprintln!("[C6]   all 30 final values match recorded history ✓");
    })
    .await
    .expect(hang_msg());
}

// C7
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_get_missing_under_load() {
    ensure_server().await;
    let p_write = Arc::new(unique_prefix("load_write"));
    let p_missing = Arc::new(unique_prefix("load_missing"));
    eprintln!("[C7] 20 writers (1000 puts) vs 20 readers probing never-written keys");

    timeout(TEST_TIMEOUT, async {
        let started = Instant::now();
        let mut handles = Vec::new();

        // 20 writers hammering prefix_write.
        for t in 0u32..20 {
            let p = Arc::clone(&p_write);
            handles.push(tokio::spawn(async move {
                for i in 0u32..50 {
                    let key = k(&p, format!("{}-{}", t, i).as_bytes());
                    put(key, format!("v{}-{}", t, i).into_bytes())
                        .await
                        .expect("put failed");
                }
            }));
        }

        // 20 readers repeatedly getting keys in prefix_missing (never written).
        for t in 0u32..20 {
            let p = Arc::clone(&p_missing);
            handles.push(tokio::spawn(async move {
                for i in 0u32..50 {
                    let key = k(&p, format!("{}-{}", t, i).as_bytes());
                    let res = get(key).await;
                    assert!(
                        res.is_err(),
                        "get on never-written key returned Ok({:?}) under load",
                        res
                    );
                }
            }));
        }

        join_handles(handles).await;
        eprintln!("[C7]   writers + missing-key probers finished in {:?} ✓", started.elapsed());
    })
    .await
    .expect(hang_msg());
}