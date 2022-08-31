use crate::error::{VertexError, VertexResult};
use bytes::Bytes;
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use model::block::BlockHash;
use model::committee::{Committee, NodePublicKey};
use model::{Round, Timestamp};
use model::vertex::{Vertex, VertexHash};
use storage::Storage;
use crate::vertex_message_handler::VertexMessage;

/// The resolution of the timer that checks whether we received replies to our sync requests, and triggers
/// new sync requests if we didn't.
const TIMER_RESOLUTION: u64 = 1_000;
/// The delay to wait before re-trying sync requests.
const SYNC_RETRY_DELAY: u128 = 1_000;
/// Determine with how many nodes to sync when re-trying to send sync-request.
const SYNC_RETRY_NODES: usize = 3;

#[derive(Debug)]
pub enum SyncMessage {
    SyncBlocks(Vec<BlockHash>, Vertex),
    SyncParentVertices(Vec<VertexHash>, Vertex),
}

/// Waits for missing parent vertices.
pub struct VertexSynchronizer {
    /// The name of this authority.
    node_key: NodePublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    storage: Storage,

    /// Receives sync commands from the `Synchronizer`.
    sync_message_receiver: Receiver<SyncMessage>,
    /// The Vertex Aggregator which can accept and process the un-sync (missing) vertices.
    vertex_sync_sender: Sender<Vertex>,

    /// Network driver allowing to send messages.
    network: SimpleSender,
    /// Store vertices for which we made sync requests in order to retry them if timeout.
    parent_requests: HashMap<VertexHash, (Round, Timestamp)>,
    /// Store pending requests for the vertex.
    pending: HashMap<VertexHash, (Round, Sender<()>)>,
    gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,
}

impl VertexSynchronizer {
    pub fn spawn(
        node_key: NodePublicKey,
        committee: Committee,
        storage: Storage,
        sync_message_receiver: Receiver<SyncMessage>,
        gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,
        vertex_sync_sender: Sender<Vertex>,
    ) {
        tokio::spawn(async move {
            Self {
                node_key,
                committee,
                storage,
                sync_message_receiver,
                vertex_sync_sender,
                gc_message_receiver,
                network: SimpleSender::new(),
                parent_requests: HashMap::new(),
                pending: HashMap::new(),
            }.run().await;
        });
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.sync_message_receiver.recv() => {
                    match message {
                        SyncMessage::SyncBlocks(_,_) => {
                            //TODO: implement sync of blocks
                        }
                        SyncMessage::SyncParentVertices(missing_parents, vertex) => {
                            debug!("Sync the parents of {}", vertex);
                            let vertex_hash = vertex.hash();
                            let round = vertex.round();
                            let owner = vertex.owner();

                            // Ensure we sync only once per vertex.
                            if self.pending.contains_key(&vertex_hash) {
                                continue;
                            }

                            // Add the vertex to the waiter pool. The waiter will return it to us
                            // when all its parents are in the store.
                            let wait_for = missing_parents
                                .iter()
                                .cloned()
                                .map(|x| (x.to_vec(), self.storage.clone()))
                                .collect();
                            let (cancel_sender, cancel_receiver) = channel(1);
                            self.pending.insert(vertex_hash, (round, cancel_sender));
                            waiting.push( Self::waiter(wait_for, vertex, cancel_receiver));

                            // Ensure we didn't already sent a sync request for these parents.
                            // Optimistically send the sync request to the node that created the vertex.
                            // If this fails (after a timeout), we broadcast the sync request.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut vertices_to_sync = Vec::new();
                            for parent in missing_parents {
                                self.parent_requests
                                    .entry(parent.clone())
                                    .or_insert_with(|| {
                                        vertices_to_sync.push(parent);
                                        (round, now)
                                    });
                            }
                            if !vertices_to_sync.is_empty() {
                                let address = self.committee
                                    .get_node_address_by_key(&owner)
                                    .expect("Vertex owner is not in committee");
                                let message = VertexMessage::VertexRequest(vertices_to_sync, self.node_key);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize VertexRequest");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(vertex)) => {
                        let _ = self.pending.remove(&vertex.hash());
                        for (hash, _) in vertex.parents() {
                            let _ = self.parent_requests.remove(hash);
                        }
                        // Send missing vertex to the Vertex Aggregator
                        self.vertex_sync_sender.send(vertex).await.expect("Failed to send vertex");
                    },
                    Ok(None) => {
                        // This request has been canceled.
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to all nodes.
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to get current time")
                        .as_millis();

                    let mut vertices_to_retry = Vec::new();
                    for (vertex_hash, (_, timestamp)) in &self.parent_requests {
                        if timestamp + SYNC_RETRY_DELAY < now {
                            debug!("Requesting sync for vertex {:?} (retry)",  base64::encode(vertex_hash));
                            vertices_to_retry.push(vertex_hash.clone());
                        }
                    }

                    if !vertices_to_retry.is_empty() {
                        let addresses = self.committee.get_node_addresses_but_me(&self.node_key);
                        let message = VertexMessage::VertexRequest(vertices_to_retry, self.node_key);
                        let bytes = bincode::serialize(&message).expect("Failed to serialize VertexRequest");
                        self.network.lucky_broadcast(addresses, Bytes::from(bytes), SYNC_RETRY_NODES).await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },

                Ok(gc_round) = self.gc_message_receiver.recv() => {
                     let mut round = gc_round;
                     for (r, handler) in self.pending.values() {
                        if r <= &mut round {
                            let _ = handler.send(()).await;
                        }
                    }
                    self.pending.retain(|_, (r, _)| r > &mut round);
                    self.parent_requests.retain(|_, (r, _)| r > &mut round);
                },
            }
        }
    }

    /// Wait for data to become available in the storage and then deliver the provided vertex.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Storage)>,
        deliver: Vertex,
        mut handler: Receiver<()>,
    ) -> VertexResult<Option<Vertex>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(VertexError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }
}
