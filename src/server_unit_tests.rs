//! Unit tests for the KvStore trait implementation.
//!
//! These tests exercise the server's RPC handler logic directly — no network,
//! no tokio::spawn, no server binary. We construct a fresh KvStore, wrap
//! arguments in tonic::Request, and await the trait method.

use super::KvStore;
use crate::rpc::kv_store::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Request;

fn new_store() -> KvStore {
    KvStore {
        db: Arc::new(RwLock::new(HashMap::new())),
    }
}

// U1
#[tokio::test]
async fn unit_example_increments() {
    use kv_store_server::KvStore as _;
    let store = new_store();
    let reply = store
        .example(Request::new(ExampleRequest { input: 41 }))
        .await
        .expect("example should succeed");
    assert_eq!(reply.into_inner().output, 42);
}

// U2
#[tokio::test]
async fn unit_echo_returns_same_string() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    for msg in ["", "hello", &"x".repeat(10_000)] {
        let reply = store
            .echo(Request::new(EchoRequest {
                req_string: msg.to_string(),
            }))
            .await
            .expect("echo should succeed");
        assert_eq!(reply.into_inner().res_string, msg);
    }
}

// U3
#[tokio::test]
async fn unit_put_then_get_roundtrips() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    store
        .put(Request::new(PutRequest {
            put_key: b"k".to_vec(),
            put_value: b"v".to_vec(),
        }))
        .await
        .expect("put should succeed");

    let reply = store
        .get(Request::new(GetRequest {
            get_key: b"k".to_vec(),
        }))
        .await
        .expect("get should succeed");
    assert_eq!(reply.into_inner().get_value, b"v".to_vec());
}

// U4
#[tokio::test]
async fn unit_get_missing_key_is_not_found() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    let err = store
        .get(Request::new(GetRequest {
            get_key: b"never_set".to_vec(),
        }))
        .await
        .expect_err("get on missing key must return Err");

    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "missing-key error must use tonic::Code::NotFound"
    );
    assert!(
        err.message().contains("Key does not exist"),
        "error message should contain 'Key does not exist', got: {:?}",
        err.message()
    );
}

// U5
#[tokio::test]
async fn unit_put_overwrites_previous_value() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    store
        .put(Request::new(PutRequest {
            put_key: b"k".to_vec(),
            put_value: b"v1".to_vec(),
        }))
        .await
        .unwrap();
    store
        .put(Request::new(PutRequest {
            put_key: b"k".to_vec(),
            put_value: b"v2".to_vec(),
        }))
        .await
        .unwrap();

    let reply = store
        .get(Request::new(GetRequest {
            get_key: b"k".to_vec(),
        }))
        .await
        .unwrap();
    assert_eq!(reply.into_inner().get_value, b"v2".to_vec());
}

// U6
#[tokio::test]
async fn unit_empty_key_roundtrips() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    store
        .put(Request::new(PutRequest {
            put_key: Vec::new(),
            put_value: b"val".to_vec(),
        }))
        .await
        .unwrap();

    let reply = store
        .get(Request::new(GetRequest {
            get_key: Vec::new(),
        }))
        .await
        .unwrap();
    assert_eq!(reply.into_inner().get_value, b"val".to_vec());
}

// U7
#[tokio::test]
async fn unit_empty_value_roundtrips() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    store
        .put(Request::new(PutRequest {
            put_key: b"k7".to_vec(),
            put_value: Vec::new(),
        }))
        .await
        .unwrap();

    let reply = store
        .get(Request::new(GetRequest {
            get_key: b"k7".to_vec(),
        }))
        .await
        .unwrap();
    assert_eq!(
        reply.into_inner().get_value,
        Vec::<u8>::new(),
        "empty value must round-trip distinctly from missing"
    );
}

// U8
#[tokio::test]
async fn unit_binary_non_utf8_roundtrips() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    let key = vec![0xff, 0x00, 0xfe, 0x80, 0x81];
    let val = vec![0x00, 0xc0, 0xc1, 0xf5, 0xff];

    store
        .put(Request::new(PutRequest {
            put_key: key.clone(),
            put_value: val.clone(),
        }))
        .await
        .unwrap();

    let reply = store
        .get(Request::new(GetRequest { get_key: key }))
        .await
        .unwrap();
    assert_eq!(reply.into_inner().get_value, val);
}

// U9
#[tokio::test]
async fn unit_large_value_1mb_roundtrips() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    let big = vec![0xab_u8; 1_048_576];
    store
        .put(Request::new(PutRequest {
            put_key: b"big".to_vec(),
            put_value: big.clone(),
        }))
        .await
        .unwrap();

    let reply = store
        .get(Request::new(GetRequest {
            get_key: b"big".to_vec(),
        }))
        .await
        .unwrap();
    let got = reply.into_inner().get_value;
    assert_eq!(got.len(), big.len());
    assert_eq!(got, big);
}

// U10
#[tokio::test]
async fn unit_many_keys_do_not_collide() {
    use kv_store_server::KvStore as _;
    let store = new_store();

    for i in 0u32..100 {
        store
            .put(Request::new(PutRequest {
                put_key: format!("key-{}", i).into_bytes(),
                put_value: format!("val-{}", i).into_bytes(),
            }))
            .await
            .unwrap();
    }

    for i in 0u32..100 {
        let reply = store
            .get(Request::new(GetRequest {
                get_key: format!("key-{}", i).into_bytes(),
            }))
            .await
            .unwrap_or_else(|e| panic!("get for key-{} failed: {:?}", i, e));
        assert_eq!(
            reply.into_inner().get_value,
            format!("val-{}", i).into_bytes(),
            "mismatch at key-{}",
            i
        );
    }
}