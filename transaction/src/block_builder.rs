use std::time::Duration;
use bytes::Bytes;
use log::{info, debug};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Instant, sleep};

use model::block::{Block, BlockHash, Transaction};
use model::committee::{Committee, NodePublicKey};
use network::ReliableSender;
use crate::handler::BlockMessage;

const TX_SIZE: usize = 1000;
const MAX_TX_COUNT_WAIT: u64 = 200;

pub struct BlockBuilder {
    node_key: NodePublicKey,
    committee: Committee,
    transaction_receiver: Receiver<Transaction>,
    serialized_block_sender: Sender<(BlockHash, Vec<u8>, NodePublicKey)>,
    current_transactions: Vec<Transaction>,
    network: ReliableSender,
}

impl BlockBuilder {
    pub fn spawn(
        node_key: NodePublicKey,
        transaction_receiver: Receiver<Transaction>,
        serialized_block_sender: Sender<(BlockHash, Vec<u8>, NodePublicKey)>,
        committee: Committee,
    ) {
        tokio::spawn(async move {
            Self {
                node_key,
                committee,
                transaction_receiver,
                serialized_block_sender,
                current_transactions: vec![],
                network: ReliableSender::new(),
            }
                .run()
                .await;
        });
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(MAX_TX_COUNT_WAIT));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(transaction) = self.transaction_receiver.recv() => {
                    // debug!("BlockBuilder received transaction");
                    self.current_transactions.push(transaction);

                    if self.current_transactions.len() >= TX_SIZE {
                        info!("BlockBuilder has enough transactions to make a block. Broadcast it to others");
                        self.build_block().await;
                        timer.as_mut().reset(Self::get_reset_time());
                    }
                },

                // When time runs out, build a block with remaining transactions in the queue
                () = &mut timer => {
                    if !self.current_transactions.is_empty() {
                         debug!("Block time runs out");
                         self.build_block().await;
                    }
                    timer.as_mut().reset(Self::get_reset_time());
                }
            }

            tokio::task::yield_now().await;
        }
    }

    async fn build_block(&mut self) {
        #[cfg(feature = "benchmark")]
            let size = self.current_transactions.len();
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_transactions
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        let block = Block::new(self.current_transactions.drain(..).collect());

        #[cfg(feature = "benchmark")]
        {
            let block_hash = block.encoded_hash();
            for id in tx_ids {
                info!(
                    "Block {} contains sample tx {}",
                    block_hash,
                    u64::from_be_bytes(id)
                );
            }

            info!("Block {} contains {} transactions", block_hash, size);
        }

        let block_hash = block.hash();
        let message = BlockMessage::Block(self.node_key, block);
        let serialized = bincode::serialize(&message).expect("Failed to serialize the block");

        // Broadcast the block through the network.
        let is_transferred = self.network.broadcast_and_wait(self.committee.get_block_receiver_addresses_but_me(&self.node_key),
                                                             Bytes::from(serialized.clone()),
                                                             self.committee.quorum_threshold()).await;

        if is_transferred {
            self.serialized_block_sender.send((block_hash, serialized.to_vec(), self.node_key)).await;
        }
    }

    fn get_reset_time() -> Instant {
        Instant::now() + Duration::from_millis(MAX_TX_COUNT_WAIT)
    }
}
