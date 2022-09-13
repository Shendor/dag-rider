use model::block::{Block, BlockHash, Transaction};
use tokio::sync::mpsc::{Sender};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::error::Error;
use model::committee::NodePublicKey;
use network::{MessageHandler, Writer};
use storage::Storage;

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockMessage {
    ProposeBlock(BlockHash, NodePublicKey),
    RegisterBlock(BlockHash, NodePublicKey),
}

#[derive(Clone)]
pub struct ReceiveBlockHandler {
    /// Sends a block to the Consensus layer so it can be added to a vertex
    pub(crate) block_sender: Sender<BlockHash>,
    /// Storage for saving blocks
    pub(crate) storage: Storage,
}

#[async_trait]
impl MessageHandler for ReceiveBlockHandler {
    async fn dispatch(&mut self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(BlockMessage::ProposeBlock(block_hash, owner)) => {
                self.block_sender
                    .send(block_hash)
                    .await
                    .expect("Failed to send block to proposer")
            },
            Ok(BlockMessage::RegisterBlock(block_hash, owner)) => {
                self.storage.write(block_hash.to_vec(), Vec::default()).await;
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}