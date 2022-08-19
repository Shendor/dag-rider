use crate::error::{VertexError, VertexResult};
use async_recursion::async_recursion;
use log::{debug, error, warn};
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use model::committee::{Committee, NodePublicKey};
use model::Round;
use model::vertex::Vertex;
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
    vertex_message_receiver: Receiver<VertexMessage>,
    /// Output all vertices to the consensus layer.
    consensus_sender: Sender<Vertex>,
    /// Sends a quorum of created vertices to the Proposer so they can be attached
    /// as parents for a new vertex
    proposer_sender: Sender<(Vec<Vertex>, Round)>,
    /// Receives our newly created vertices from the `Proposer`.
    proposer_receiver: Receiver<Vertex>,
    /// Send commands to the `VertexSynchronizer`.
    vertex_sync_sender: Sender<SyncMessage>,
    /// Receives loopback vertices from the `VertexSynchronizer`.
    vertex_sync_receiver: Receiver<Vertex>,

    /// Aggregates vertices to use as parents for a new vertex.
    new_vertices: HashMap<Round, Vec<Vertex>>,
}

impl VertexAggregator {
    pub fn spawn(
        node_key: NodePublicKey,
        committee: Committee,
        storage: Storage,
        vertex_message_receiver: Receiver<VertexMessage>,
        proposer_receiver: Receiver<Vertex>,
        consensus_sender: Sender<Vertex>,
        proposer_sender: Sender<(Vec<Vertex>, Round)>,
        vertex_sync_sender: Sender<SyncMessage>,
        vertex_sync_receiver: Receiver<Vertex>,
    ) {
        tokio::spawn(async move {
            Self {
                node_key,
                committee,
                storage,
                vertex_message_receiver,
                consensus_sender,
                proposer_sender,
                proposer_receiver,
                vertex_sync_sender,
                vertex_sync_receiver,
                new_vertices: HashMap::new(),
            }.run().await;
        });
    }

    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other nodes.
                Some(message) = self.vertex_message_receiver.recv() => {
                    match message {
                        VertexMessage::NewVertex(vertex) => {
                            self.process_vertex(&vertex).await
                        },
                        _ => panic!("Unexpected message received in Vertex Aggregator")
                    }
                },

                // We receive here loopback vertices from the `VertexSynchronizer`. Those are vertices for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(vertex) = self.vertex_sync_receiver.recv() => self.process_vertex(&vertex).await,

                // Receive new vertices from the Proposer.
                Some(vertex) = self.proposer_receiver.recv() => self.process_vertex(&vertex).await,
            };

            match result {
                Ok(()) => (),
                Err(e) => error!("{:?}", e),
            }
        }
    }

    #[async_recursion]
    async fn process_vertex(&mut self, vertex: &Vertex) -> VertexResult<()> {
        debug!("Processing {:?}", vertex);
        let vertex_hash = vertex.hash();
        let round = vertex.round();

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns an empty
        // vector. It will gather the missing parents (as well as all ancestors) from other nodes and then
        // reschedule processing of this header.
        let parents = self.get_parents(vertex).await?;
        if parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", vertex.encoded_hash());
            return Ok(());
        }

        if parents.len() >= self.committee.quorum_threshold() {
            return Err(VertexError::VertexParentsQuorumFailed(vertex.encoded_hash(), self.committee.quorum_threshold()));
        }

        // Store the vertex.
        let bytes = bincode::serialize(vertex).expect("Failed to serialize vertex");
        self.storage.write(vertex_hash.to_vec(), bytes).await;

        // Check if we have enough vertices to enter a new dag round and propose a new vertex.
        if let Some(parents) = self.add_vertex(vertex.clone())
        {
            self.proposer_sender
                .send((parents, round))
                .await
                .expect("Failed to send parents for the vertex round");
        }

        // Send it to the consensus layer.
        if let Err(e) = self.consensus_sender.send(vertex.clone()).await {
            warn!("Failed to deliver vertex {} to the consensus: {}", vertex.encoded_hash(), e);
        }

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
    async fn get_parents(&mut self, vertex: &Vertex) -> VertexResult<Vec<Vertex>> {
        let mut missing_vertices = Vec::new();
        let mut parents = Vec::new();
        for (vertex_hash, _) in vertex.parents() {
            match self.storage.read(vertex_hash.to_vec()).await? {
                Some(raw_vertex) => parents.push(raw_vertex),
                None => missing_vertices.push(vertex_hash.clone()),
            }
        }

        if missing_vertices.is_empty() {
            return Ok(parents.iter().map(|p| bincode::deserialize(&p).unwrap()).collect());
        } else {
            self.vertex_sync_sender
                .send(SyncMessage::SyncParentVertices(missing_vertices, vertex.clone()))
                .await
                .expect("Failed to send sync parents request");
            Ok(Vec::new())
        }
    }
}
