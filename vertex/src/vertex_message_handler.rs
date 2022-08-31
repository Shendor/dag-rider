use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use log::debug;
use tokio::sync::mpsc::{Sender};
use model::committee::{Committee, NodePublicKey};
use serde::{Deserialize, Serialize};
use model::vertex::{Vertex, VertexHash};
use network::{MessageHandler, SimpleSender, Writer};
use storage::Storage;

#[derive(Debug, Serialize, Deserialize)]
pub enum VertexMessage {
    NewVertex(Vertex),
    VertexRequest(Vec<VertexHash>, NodePublicKey),
}

#[derive(Clone)]
pub struct VertexReceiverHandler {
    /// Vertex sender to the Vertex Aggregator
    pub vertex_sender: Sender<Vertex>,
    pub committee: Committee,
    pub storage: Storage,
    pub network: SimpleSender,
}

impl VertexReceiverHandler {
    pub fn new(vertex_sender: Sender<Vertex>,
               committee: Committee,
               storage: Storage) -> Self {
        Self {
            vertex_sender,
            committee,
            storage,
            network: SimpleSender::new()
        }
    }
}

#[async_trait]
impl MessageHandler for VertexReceiverHandler {

    async fn dispatch(&mut self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized).map_err(model::Error::SerializationError)? {
            VertexMessage::VertexRequest(vertices_to_sync, from) => {
                debug!("Received a VertexRequest message from the synchronizer");
                if let Some(address) = self.committee.get_node_address_by_key(&from) {
                    for vertex_hash in vertices_to_sync {
                        match self.storage.read(vertex_hash.to_vec()).await? {
                            Some(data) => {
                                debug!("Found the un-sync vertex in the storage. Send it back to node {}", address);
                                let vertex = bincode::deserialize(&data)
                                    .expect("Failed to deserialize vertex from the storage");
                                let bytes = bincode::serialize(&VertexMessage::NewVertex(vertex))
                                     .expect("Failed to serialize vertex from the storage");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                            None => (),
                        }
                    }
                }
            }
            VertexMessage::NewVertex(mut vertex) => {
                debug!("Received a broadcast NewVertex message. Re-routing the vertex to Vertex Aggregator");
                vertex.reset_to_current_time();
                self.vertex_sender
                    .send(vertex)
                    .await
                    .expect("Failed to send vertex to Vertex Aggregator")
            }
        }
        Ok(())
    }
}