//! The gRPC server.
//!

use crate::{log, rpc::kv_store::*, SERVER_ADDR};
use anyhow::Result;
use tonic::{transport::Server, Request, Response, Status};
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::sync::Arc;


pub struct KvStore {
    // HashMap storing the K-V
    db: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

#[tonic::async_trait]
impl kv_store_server::KvStore for KvStore {
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        log::info!("Received example request.");
        Ok(Response::new(ExampleReply {
            output: req.into_inner().input + 1,
        }))
    }

    // TODO: RPC implementation
    async fn echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<EchoReply>, Status> {
        log::info!("Received echo request.");
        Ok(Response::new(EchoReply {
            res_string: req.into_inner().req_string,
        }))
    }
    async fn put(
        &self,
        req: Request<PutRequest>,
    ) -> Result<Response<PutReply>, Status> {
        log::info!("Received put request.");
        let inner: PutRequest = req.into_inner();
        let mut guard = self.db.write().await;
        guard.insert(inner.put_key, inner.put_value); 
        Ok(Response::new(PutReply {
        }))
    }
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        log::info!("Received get request.");
        let inner: GetRequest = req.into_inner();
        let guard = self.db.read().await;
        match guard.get(&inner.get_key){
            Some(v) => Ok(Response::new(GetReply{get_value: v.clone() })),
            None    => Err(Status::not_found("Key does not exist.")),
        }

    }
}

pub async fn start() -> Result<()> {
    // let svc = kv_store_server::KvStoreServer::new(KvStore {});
    let svc = kv_store_server::KvStoreServer::new(KvStore {db: Arc::new(RwLock::new(HashMap::new())),});

    log::info!("Starting KV store server.");
    Server::builder()
        .add_service(svc)
        .serve(SERVER_ADDR.parse().unwrap())
        .await?;
    Ok(())
}

pub async fn start_on(addr: &str) -> Result<()> {
    let svc = kv_store_server::KvStoreServer::new(KvStore {
        db: Arc::new(RwLock::new(HashMap::new())),
    });

    log::info!("Starting KV store server on {}.", addr);
    Server::builder()
        .add_service(svc)
        .serve(addr.parse().unwrap())
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "server_unit_tests.rs"]
mod server_unit_tests;
