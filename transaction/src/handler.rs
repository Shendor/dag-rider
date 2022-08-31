use model::block::{Block, BlockHash, Transaction};
use tokio::sync::mpsc::{Sender};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::error::Error;
use network::{MessageHandler, Writer};
use storage::Storage;

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockMessage {
    Block(Block),
}

#[derive(Clone)]
pub struct ReceiveTxHandler {
    /// Sends transactions to [Block Builder][crate::block_builder::BlockBuilder]
    /// so they can be added to a block
    pub(crate) transaction_sender: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for ReceiveTxHandler {
    async fn dispatch(&mut self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        info!("TxReceiverHandler received transaction to process {:?}", message);
        // Send the transaction to the block builder.
        self.transaction_sender
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        Ok(())
    }
}

#[derive(Clone)]
pub struct ReceiveBlockHandler {
    /// Sends a block to the Consensus layer so it can be added to a vertex
    pub(crate) block_sender: Sender<BlockHash>,
    /// Storage for saving blocks
    pub(crate) storage: Storage
}

#[async_trait]
impl MessageHandler for ReceiveBlockHandler {
    async fn dispatch(&mut self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(BlockMessage::Block(block)) => {
                info!("Received a block to process with {} transactions. Sending it to Proposer", block.transactions.len());
                self.storage.write(block.hash().to_vec(), serialized.to_vec()).await;

                self.block_sender
                    .send(block.hash())
                    .await
                    .expect("Failed to send block")
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}