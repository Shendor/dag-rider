use crate::error::{VertexError, VertexResult};
use async_recursion::async_recursion;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use model::committee::{Committee, NodePublicKey};
use model::Round;
use model::vertex::{Vertex, VertexHash};
use storage::Storage;
use crate::vertex_message_handler::VertexMessage;
use crate::vertex_synchronizer::SyncMessage;

pub struct VertexAggregator {
    /// The public key of this node.
    node_key: NodePublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    storage: Storage,

    /// Receiver for dag messages.
    vertex_receiver: Receiver<Vertex>,
    /// Output all vertices to the consensus layer.
    consensus_sender: Sender<Vertex>,
    /// Sends a quorum of created vertices to the Proposer so they can be attached
    /// as parents for a new vertex.
    parents_sender: Sender<(Vec<Vertex>, Round)>,
    /// Receives our newly created vertices from the `Proposer`.
    proposer_receiver: Receiver<Vertex>,
    /// Send commands to the `VertexSynchronizer`.
    vertex_sync_sender: Sender<SyncMessage>,
    /// Receives loopback vertices from the `VertexSynchronizer`.
    vertex_sync_receiver: Receiver<Vertex>,

    /// Aggregates vertices to use as parents for a new vertex.
    new_vertices: HashMap<Round, Vec<Vertex>>,
    gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,

}

impl VertexAggregator {
    pub fn spawn(
        node_key: NodePublicKey,
        committee: Committee,
        storage: Storage,
        vertex_receiver: Receiver<Vertex>,
        parents_sender: Sender<(Vec<Vertex>, Round)>,
        proposer_receiver: Receiver<Vertex>,
        consensus_sender: Sender<Vertex>,
        vertex_sync_sender: Sender<SyncMessage>,
        vertex_sync_receiver: Receiver<Vertex>,
        gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,
    ) {
        tokio::spawn(async move {
            Self {
                node_key,
                committee,
                storage,
                vertex_receiver,
                parents_sender,
                proposer_receiver,
                consensus_sender,
                vertex_sync_sender,
                vertex_sync_receiver,
                gc_message_receiver,
                new_vertices: HashMap::new(),
            }.run().await;
        });
    }

    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other nodes.
                Some(vertex) = self.vertex_receiver.recv() => {
                    self.process_vertex(vertex).await
                },

                // We receive here loopback vertices from the `VertexSynchronizer`. Those are vertices for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                // Some(vertex) = self.vertex_sync_receiver.recv() => self.process_vertex(&vertex).await,

                // Receive new vertices from the Proposer.
                Some(vertex) = self.proposer_receiver.recv() => self.process_vertex(vertex).await,
                Result::Ok(gc_round) = self.gc_message_receiver.recv() => {
                    debug!("GC round: {}", gc_round);
                    Ok(())
                },
            };

            match result {
                Ok(()) => (),
                Err(e) => error!("Unexpected error: {:?}", e),
            }
        }
    }

    #[async_recursion]
    async fn process_vertex(&mut self, vertex: Vertex) -> VertexResult<()> {
        debug!("Processing vertex {}", vertex.encoded_hash());
        let vertex_hash = vertex.hash();
        let round = vertex.round();

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns true
        // which means that synchronization is needed and it will gather the missing parents
        // (as well as all ancestors) from other nodes and then reschedule processing of this header.
        if self.sync_parents(&vertex).await? {
            debug!("Parents not sync for vertex {}. Suspend its processing until it's sync", vertex.encoded_hash());
            return Ok(());
        }

        // Store the vertex.
        let bytes = bincode::serialize(&vertex).expect("Failed to serialize vertex");
        self.storage.write(vertex_hash.to_vec(), bytes).await;

        // Check if we have enough vertices to enter a new dag round and propose a new vertex.
        if let Some(parents) = self.add_vertex(vertex.clone())
        {
            info!("Received enough parents for round {}. Sending it to the Proposer", round);
            self.parents_sender
                .send((parents, round))
                .await
                .expect("Failed to send parents for the vertex round");
        }

        // Send it to the consensus layer.
        self.consensus_sender.send(vertex).await;

        Ok(())
    }

    fn add_vertex(&mut self, vertex: Vertex) -> Option<Vec<Vertex>> {
        let round = vertex.round();
        self.new_vertices
            .entry(round)
            .or_insert_with(|| vec![])
            .push(vertex);
        if self.new_vertices.get(&round).unwrap().len() >= self.committee.quorum_threshold() {
            // we have enough vertices for the current round to return
            Some(self.new_vertices.get_mut(&round).unwrap().drain(..).collect())
        } else {
            None
        }
    }

    /// Returns the parents of a vertex if we have them all. If at least one parent is missing,
    /// we return an empty vector, synchronize with other nodes, and re-schedule processing
    /// of the vertex for when we will have all the parents.
    async fn sync_parents(&mut self, vertex: &Vertex) -> VertexResult<bool> {
        let mut missing_vertices: Vec<VertexHash> = Vec::new();
        let mut parents = Vec::new();
        for (parent, _) in vertex.parents() {
            match self.storage.read(parent.to_vec()).await? {
                Some(raw_vertex) => parents.push(raw_vertex),
                None => missing_vertices.push(parent.clone()),
            }
        }

        if missing_vertices.is_empty() {
            return if parents.len() < self.committee.quorum_threshold() {
                 Err(VertexError::VertexParentsQuorumFailed(vertex.encoded_hash(), self.committee.quorum_threshold()))
            } else {
                 Ok(false)
            }
        } else {
            warn!("Not all parents found in the storage for vertex '{}'. Start to synchronize...", vertex.encoded_hash());
            // self.vertex_sync_sender
            //     .send(SyncMessage::SyncParentVertices(missing_vertices, vertex.clone()))
            //     .await
            //     .expect("Failed to send sync parents request");
            Ok(true)
        }
    }
}
