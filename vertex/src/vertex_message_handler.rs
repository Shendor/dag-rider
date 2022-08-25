use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use log::debug;
use tokio::sync::mpsc::{Sender};
use model::committee::NodePublicKey;
use serde::{Deserialize, Serialize};
use model::vertex::{Vertex, VertexHash};
use network::{MessageHandler, Writer};

#[derive(Debug, Serialize, Deserialize)]
pub enum VertexMessage {
    NewVertex(Vertex),
    VertexRequest(Vec<VertexHash>, NodePublicKey),
}

#[derive(Clone)]
pub struct VertexReceiverHandler {
    /// Vertex sender to the Vertex Aggregator
    pub vertex_sender: Sender<Vertex>,
}

#[async_trait]
impl MessageHandler for VertexReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized).map_err(model::Error::SerializationError)? {
            VertexMessage::VertexRequest(_vertices_to_sync, _from) => {
                debug!("Received a VertexRequest message from the synchronizer");
                //TODO: Sync vertex
            },
            VertexMessage::NewVertex(vertex) => {
                debug!("Received a broadcast NewVertex message. Re-routing the vertex to Vertex Aggregator");
                self.vertex_sender
                    .send(vertex)
                    .await
                    .expect("Failed to send vertex to Vertex Aggregator")
            },
        }
        Ok(())
    }
}