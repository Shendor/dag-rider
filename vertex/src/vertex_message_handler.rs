use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use tokio::sync::mpsc::{Sender};

use model::vertex::{Vertex};
use network::{MessageHandler, Writer};

#[derive(Clone)]
pub struct VertexReceiverHandler {
    pub vertex_to_consensus_sender: Sender<Vertex>,
}

#[async_trait]
impl MessageHandler for VertexReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized).map_err(model::Error::SerializationError)? {
            vertex => self
                .vertex_to_consensus_sender
                .send(vertex)
                .await
                .expect("Failed to send vertex to consensus"),
        }
        Ok(())
    }
}