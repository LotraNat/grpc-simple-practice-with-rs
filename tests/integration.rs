//! End-to-end integration tests: client ↔ in-process server over the wire.

mod common;

use common::{ensure_server, k, unique_prefix};
use lab_grpc::client::{echo, example, get, put};

// I1
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_echo_roundtrip() {
    ensure_server().await;

    for msg in ["", "hello", &"x".repeat(5_000)] {
        let got = echo(msg.to_string()).await.expect("echo should succeed");
        assert_eq!(got, msg);
    }
}

// I2
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_example_wire() {
    ensure_server().await;

    assert_eq!(example(0).await.unwrap(), 1);
    assert_eq!(example(u32::MAX - 1).await.unwrap(), u32::MAX);
}

// I3
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_put_then_get() {
    ensure_server().await;
    let p = unique_prefix("put_then_get");

    put(k(&p, b"a"), b"apple".to_vec()).await.unwrap();
    let v = get(k(&p, b"a")).await.unwrap();
    assert_eq!(v, b"apple".to_vec());
}

// I4
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_get_missing_key_is_err() {
    ensure_server().await;
    let p = unique_prefix("missing_key");

    let res = get(k(&p, b"never_set")).await;
    assert!(
        res.is_err(),
        "get on missing key must return Err, got Ok({:?})",
        res
    );
}

// I5
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_put_overwrite() {
    ensure_server().await;
    let p = unique_prefix("put_overwrite");
    let key = k(&p, b"overwrite");

    put(key.clone(), b"first".to_vec()).await.unwrap();
    put(key.clone(), b"second".to_vec()).await.unwrap();

    let v = get(key).await.unwrap();
    assert_eq!(v, b"second".to_vec());
}

// I6
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_binary_key_and_value() {
    ensure_server().await;
    // Keys here are auto-namespaced because we prefix them.
    let p = unique_prefix("binary");
    let key = k(&p, &[39, 127, 254, 0, 1, 2, 78]);
    let val = vec![1u8, 4, 0, 244, 3, 33, 68];

    put(key.clone(), val.clone()).await.unwrap();
    let got = get(key).await.unwrap();
    assert_eq!(got, val);
}

// I7
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_empty_key_and_empty_value() {
    ensure_server().await;
    let p = unique_prefix("empty");

    // Empty VALUE (namespaced key).
    let key_a = k(&p, b"empty-val");
    put(key_a.clone(), Vec::new()).await.unwrap();
    assert_eq!(get(key_a).await.unwrap(), Vec::<u8>::new());

    // Empty KEY cannot be namespaced — just trust it's unique (no other test
    // puts to a literal empty key).
    put(Vec::new(), b"sentinel".to_vec()).await.unwrap();
    assert_eq!(get(Vec::new()).await.unwrap(), b"sentinel".to_vec());
}

// I8
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_many_sequential_writes_then_reads() {
    ensure_server().await;
    let p = unique_prefix("sequential");

    for i in 0u32..50 {
        put(
            k(&p, format!("{}", i).as_bytes()),
            format!("v{}", i).into_bytes(),
        )
        .await
        .unwrap();
    }
    for i in 0u32..50 {
        let got = get(k(&p, format!("{}", i).as_bytes())).await.unwrap();
        assert_eq!(got, format!("v{}", i).into_bytes(), "mismatch at i={}", i);
    }
}

// I9
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_key_case_sensitivity() {
    ensure_server().await;
    let p = unique_prefix("case");

    let kupper = k(&p, b"Key");
    let klower = k(&p, b"key");

    put(kupper.clone(), b"upper".to_vec()).await.unwrap();
    // Before lower is put, lowercase must be a Err (distinct key).
    assert!(
        get(klower.clone()).await.is_err(),
        "lowercase lookup must be distinct from uppercase"
    );

    put(klower.clone(), b"lower".to_vec()).await.unwrap();
    assert_eq!(get(kupper).await.unwrap(), b"upper".to_vec());
    assert_eq!(get(klower).await.unwrap(), b"lower".to_vec());
}

// I10
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_get_does_not_mutate() {
    ensure_server().await;
    let p = unique_prefix("get_pure");
    let key = k(&p, b"persistent");

    put(key.clone(), b"stable".to_vec()).await.unwrap();
    for _ in 0..3 {
        assert_eq!(get(key.clone()).await.unwrap(), b"stable".to_vec());
    }
    // Sibling never written.
    assert!(get(k(&p, b"sibling")).await.is_err());
}