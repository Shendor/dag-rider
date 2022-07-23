use tokio::sync::mpsc::{Receiver, Sender};
use model::vertex::{Vertex, VertexHash, Header};
use model::config::Parameters;
use model::staker::NodePublicKey;
use model::vote::Vote;
use serde::{Deserialize, Serialize};
use std::error::Error;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use crate::core::Core;

#[derive(Debug, Serialize, Deserialize)]
pub enum VertexMessage {
    Header(Header),
    Vote(Vote),
    Vertex(Vertex),
    VertexRequest(Vec<VertexHash>, NodePublicKey),
}

#[derive(Clone)]
pub struct VertexReceiverHandler {
    pub vertex_message_sender: Sender<VertexMessage>,
    pub vertex_request_sender: Sender<(Vec<VertexHash>, NodePublicKey)>,
}

#[async_trait]
impl MessageHandler for VertexReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized).map_err(model::Error::SerializationError)? {
            VertexMessage::VertexRequest(missing, requestor) => self
                .vertex_request_sender
                .send((missing, requestor))
                .await
                .expect("Failed to send vertex message"),
            request => self
                .vertex_message_sender
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}