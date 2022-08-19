use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
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
    pub vertex_message_sender: Sender<VertexMessage>,
}

#[async_trait]
impl MessageHandler for VertexReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized).map_err(model::Error::SerializationError)? {
            VertexMessage::VertexRequest(_vertices_to_sync, _from) => {
                //TODO: Sync vertex
            },
            vertex => self
                .vertex_message_sender
                .send(vertex)
                .await
                .expect("Failed to send vertex to consensus"),
        }
        Ok(())
    }
}