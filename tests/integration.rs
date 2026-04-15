//! End-to-end integration tests: client ↔ in-process server over the wire.
//!
//! Run with `cargo test --test integration -- --nocapture` to see the
//! per-test progress log.

mod common;

use common::{ensure_server, k, unique_prefix};
use lab_grpc::client::{echo, example, get, put};

// I1
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_echo_roundtrip() {
    ensure_server().await;
    eprintln!("[I1] echo roundtrip: empty / short / 5KB payloads");

    for msg in ["", "hello", &"x".repeat(5_000)] {
        let got = echo(msg.to_string()).await.expect("echo should succeed");
        assert_eq!(got, msg);
        eprintln!("[I1]   echoed {} bytes ✓", msg.len());
    }
    eprintln!("[I1] done");
}

// I2
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_example_wire() {
    ensure_server().await;
    eprintln!("[I2] example(n) should return n+1 across the wire");

    let r1 = example(0).await.unwrap();
    assert_eq!(r1, 1);
    eprintln!("[I2]   example(0) = {} ✓", r1);

    let r2 = example(u32::MAX - 1).await.unwrap();
    assert_eq!(r2, u32::MAX);
    eprintln!("[I2]   example(u32::MAX - 1) = {} ✓", r2);
    eprintln!("[I2] done");
}

// I3
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_put_then_get() {
    ensure_server().await;
    let p = unique_prefix("put_then_get");
    eprintln!("[I3] put(\"a\" -> \"apple\") then get");

    put(k(&p, b"a"), b"apple".to_vec()).await.unwrap();
    let v = get(k(&p, b"a")).await.unwrap();
    assert_eq!(v, b"apple".to_vec());
    eprintln!("[I3]   got back {:?} ✓", String::from_utf8_lossy(&v));
}

// I4
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_get_missing_key_is_err() {
    ensure_server().await;
    let p = unique_prefix("missing_key");
    eprintln!("[I4] get on never-written key should return Err");

    let res = get(k(&p, b"never_set")).await;
    assert!(
        res.is_err(),
        "get on missing key must return Err, got Ok({:?})",
        res
    );
    eprintln!("[I4]   got Err as expected ✓");
}

// I5
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_put_overwrite() {
    ensure_server().await;
    let p = unique_prefix("put_overwrite");
    let key = k(&p, b"overwrite");
    eprintln!("[I5] put \"first\", then overwrite with \"second\"");

    put(key.clone(), b"first".to_vec()).await.unwrap();
    put(key.clone(), b"second".to_vec()).await.unwrap();

    let v = get(key).await.unwrap();
    assert_eq!(v, b"second".to_vec());
    eprintln!("[I5]   final value = {:?} ✓", String::from_utf8_lossy(&v));
}

// I6
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_binary_key_and_value() {
    ensure_server().await;
    // Keys here are auto-namespaced because we prefix them.
    let p = unique_prefix("binary");
    let key = k(&p, &[39, 127, 254, 0, 1, 2, 78]);
    let val = vec![1u8, 4, 0, 244, 3, 33, 68];
    eprintln!("[I6] non-UTF8 binary key ({}B) + binary value ({}B)", key.len(), val.len());

    put(key.clone(), val.clone()).await.unwrap();
    let got = get(key).await.unwrap();
    assert_eq!(got, val);
    eprintln!("[I6]   roundtripped {} bytes verbatim ✓", got.len());
}

// I7
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_empty_key_and_empty_value() {
    ensure_server().await;
    let p = unique_prefix("empty");
    eprintln!("[I7] edge cases: empty value and empty key");

    // Empty VALUE (namespaced key).
    let key_a = k(&p, b"empty-val");
    put(key_a.clone(), Vec::new()).await.unwrap();
    assert_eq!(get(key_a).await.unwrap(), Vec::<u8>::new());
    eprintln!("[I7]   empty value stored and retrieved ✓");

    // Empty KEY cannot be namespaced — just trust it's unique (no other test
    // puts to a literal empty key).
    put(Vec::new(), b"sentinel".to_vec()).await.unwrap();
    assert_eq!(get(Vec::new()).await.unwrap(), b"sentinel".to_vec());
    eprintln!("[I7]   empty key accepted ✓");
}

// I8
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_many_sequential_writes_then_reads() {
    ensure_server().await;
    let p = unique_prefix("sequential");
    eprintln!("[I8] 50 sequential puts, then 50 sequential gets");

    for i in 0u32..50 {
        put(
            k(&p, format!("{}", i).as_bytes()),
            format!("v{}", i).into_bytes(),
        )
        .await
        .unwrap();
    }
    eprintln!("[I8]   50 puts done");
    for i in 0u32..50 {
        let got = get(k(&p, format!("{}", i).as_bytes())).await.unwrap();
        assert_eq!(got, format!("v{}", i).into_bytes(), "mismatch at i={}", i);
    }
    eprintln!("[I8]   50 gets verified ✓");
}

// I9
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_key_case_sensitivity() {
    ensure_server().await;
    let p = unique_prefix("case");
    eprintln!("[I9] \"Key\" and \"key\" must be distinct entries");

    let kupper = k(&p, b"Key");
    let klower = k(&p, b"key");

    put(kupper.clone(), b"upper".to_vec()).await.unwrap();
    // Before lower is put, lowercase must be a Err (distinct key).
    assert!(
        get(klower.clone()).await.is_err(),
        "lowercase lookup must be distinct from uppercase"
    );
    eprintln!("[I9]   \"key\" absent while only \"Key\" is set ✓");

    put(klower.clone(), b"lower".to_vec()).await.unwrap();
    assert_eq!(get(kupper).await.unwrap(), b"upper".to_vec());
    assert_eq!(get(klower).await.unwrap(), b"lower".to_vec());
    eprintln!("[I9]   both cases store distinct values ✓");
}

// I10
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_get_does_not_mutate() {
    ensure_server().await;
    let p = unique_prefix("get_pure");
    let key = k(&p, b"persistent");
    eprintln!("[I10] repeated gets must not mutate or create keys");

    put(key.clone(), b"stable".to_vec()).await.unwrap();
    for _ in 0..3 {
        assert_eq!(get(key.clone()).await.unwrap(), b"stable".to_vec());
    }
    eprintln!("[I10]  3× gets all returned \"stable\" ✓");
    // Sibling never written.
    assert!(get(k(&p, b"sibling")).await.is_err());
    eprintln!("[I10]  sibling key still absent ✓");
}