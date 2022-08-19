use log::{debug, info, warn};
use std::cmp::Ordering;
use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use model::block::BlockHash;
use model::committee::{Committee, NodePublicKey};
use model::Round;
use model::vertex::Vertex;
use network::ReliableSender;
use crate::vertex_message_handler::VertexMessage;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    node_key: NodePublicKey,
    /// The committee information.
    committee: Committee,
    /// The maximum delay to wait for batches' digests.
    max_vertex_delay: u64,

    /// Receives vertices of the round which can be a parent to a new one for proposal
    vertices_receiver: Receiver<(Vec<Vertex>, Round)>,
    /// Sends a new vertex to the Vertex Aggregator
    vertex_sender: Sender<Vertex>,
    /// Receives the block hashes from the Block Builder.
    block_receiver: Receiver<BlockHash>,

    network: ReliableSender,
    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Vertex>,
    /// Holds the certificate of the last leader (if any).
    last_leader: Option<Vertex>,
    /// Holds the blocks' hashes waiting to be included in the next vertex.
    blocks: Vec<BlockHash>,
}

impl Proposer {
    pub fn spawn(
        node_key: NodePublicKey,
        committee: Committee,
        header_size: usize,
        max_header_delay: u64,
        vertices_receiver: Receiver<(Vec<Vertex>, Round)>,
        vertex_sender: Sender<Vertex>,
        block_receiver: Receiver<BlockHash>,
        network: ReliableSender,
    ) {
        let keys = committee.validators.iter().map(|(_, v)| v.public_key.clone()).collect();
        let genesis = Vertex::genesis(keys);
        tokio::spawn(async move {
            Self {
                node_key,
                committee,
                max_vertex_delay: max_header_delay,
                vertices_receiver,
                vertex_sender,
                block_receiver,
                network,
                round: 0,
                last_parents: genesis,
                last_leader: None,
                blocks: Vec::with_capacity(2 * header_size),
            }
            .run()
            .await;
        });
    }

    /// Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);
        let mut can_proceed = true;

        let timer = sleep(Duration::from_millis(self.max_vertex_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new vertex. We propose a new vertex when we have a quorum of parents
            // and one of the following conditions is met:
            // (i) the timer expired (we timed out on the leader or gave up gather votes for the leader),
            // (ii) we have enough blocks and we are on the happy path (we can vote for
            // the leader or the leader has enough votes to enable a commit).
            let enough_parents = !self.last_parents.is_empty();
            let timer_expired = timer.is_elapsed();

            if (timer_expired || (!self.blocks.is_empty() && can_proceed)) && enough_parents {
                if timer_expired {
                    warn!("Timer expired for round {}", self.round);
                }

                // Advance to the next round.
                self.round += 1;
                debug!("Dag moved to round {}", self.round);

                self.create_vertex().await;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_vertex_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // Receive vertices from the Vertex Aggregator when it reaches the quorum
                // and use these vertices as parents for a future proposed vertex.
                Some((parents, round)) = self.vertices_receiver.recv() => {
                    // Compare the parents' round number with our current round.
                    match round.cmp(&self.round) {
                        Ordering::Greater => {
                            // We accept round bigger than our current round to jump ahead in case we were
                            // late (or just joined the network).
                            self.round = round;
                            self.last_parents = parents;
                        },
                        Ordering::Less => {
                            // Ignore parents from older rounds.
                        },
                        Ordering::Equal => {
                            // The core gives us the parents the first time they are enough to form a quorum.
                            // Then it keeps giving us all the extra parents.
                            self.last_parents.extend(parents)
                        }
                    }

                    // Check whether we can advance to the next round. Note that if we timeout,
                    // we ignore this check and advance anyway.
                    can_proceed = match self.round % 2 {
                        0 => self.update_leader(),
                        _ => self.enough_votes(),
                    }
                }
                // Receive blocks from Block component
                Some(block_hash) = self.block_receiver.recv() => {
                    self.blocks.push(block_hash);
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }

    async fn create_vertex(&mut self) {
        let vertex = Vertex::new(
            self.node_key,
            self.round,
            self.blocks.drain(..).collect(),
            self.last_parents.drain(..).map(|x| (x.hash(), x.round())).collect(),
        );

        let addresses = self.committee.get_node_addresses();
        let bytes = bincode::serialize(&VertexMessage::NewVertex(vertex))
            .expect("Failed to serialize the new vertex");
        self.network.broadcast(addresses, Bytes::from(bytes)).await;
    }

    /// Update the last leader.
    fn update_leader(&mut self) -> bool {
        let leader_name = self.committee.leader(self.round as usize);
        self.last_leader = self
            .last_parents
            .iter()
            .find(|x| x.owner() == leader_name)
            .cloned();

        if let Some(leader) = self.last_leader.as_ref() {
            debug!("Got leader {} for round {}", leader.encoded_owner(), self.round);
        }

        self.last_leader.is_some()
    }

    /// Check whether if we have (i) 2f+1 votes for the leader, (ii) f+1 nodes not voting for the leader,
    /// or (iii) there is no leader to vote for.
    fn enough_votes(&self) -> bool {
        let leader = match &self.last_leader {
            Some(x) => x.hash(),
            None => return true,
        };

        let mut votes_for_leader = 0;
        let mut no_votes = 0;
        for vertex in &self.last_parents {
            if vertex.parents().contains_key(&leader) {
                votes_for_leader += 1;
            } else {
                no_votes += 1;
            }
        }

        let mut enough_votes = votes_for_leader >= self.committee.quorum_threshold();
        if enough_votes {
            if let Some(leader) = self.last_leader.as_ref() {
                info!(
                    "Got enough support for leader {} at round {}",
                    leader.encoded_owner(),
                    self.round
                );
            }
        }
        enough_votes |= no_votes >= self.committee.validity_threshold();
        enough_votes
    }
}
