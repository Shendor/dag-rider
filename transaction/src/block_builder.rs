use std::time::Duration;
use bytes::Bytes;
use log::{info, debug};
use tokio::sync::mpsc::{Receiver};
use tokio::time::{Instant, sleep};

use model::block::{Block, Transaction};
use model::committee::Committee;
use network::ReliableSender;
use crate::handler::BlockMessage;

const TX_SIZE: usize = 1000;
const MAX_TX_COUNT_WAIT: u64 = 5000;

pub struct BlockBuilder {
    committee: Committee,
    transaction_receiver: Receiver<Transaction>,
    current_transactions: Vec<Transaction>,
    network: ReliableSender,
}

impl BlockBuilder {
    pub fn spawn(
        transaction_receiver: Receiver<Transaction>,
        committee: Committee,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                transaction_receiver,
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
                    debug!("BlockBuilder received transaction");
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

        let message = BlockMessage::Block(block);
        let serialized = bincode::serialize(&message).expect("Failed to serialize the block");

        // Broadcast the block through the network.
        let is_transferred = self.network.broadcast_and_wait(self.committee.get_block_receiver_addresses(),
                                                             Bytes::from(serialized),
                                                             self.committee.quorum_threshold()).await;

        if is_transferred {
            info!("Block has been broadcast")
        }
    }

    fn get_reset_time() -> Instant {
        Instant::now() + Duration::from_millis(MAX_TX_COUNT_WAIT)
    }
}
