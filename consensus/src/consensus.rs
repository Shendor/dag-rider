use std::collections::{BTreeSet, HashMap};
use log::{debug, error, info, warn};
use tokio::count;
use tokio::sync::mpsc::{Receiver, Sender};
use model::committee::Committee;
use model::{Round, Timestamp};
use model::vertex::Vertex;
use crate::garbage_collector::GarbageCollector;
use crate::state::State;

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    state: State,
    gc_service: GarbageCollector,

    /// Receives new vertices from the `VertexAggregator`.
    vertex_receiver: Receiver<Vertex>,

    // ordered_vertex_timestamps_sender: Sender<(Vertex, HashMap<Round, BTreeSet<Timestamp>>)>,
    // gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,
}

const WAVE: u64 = 2;

impl Consensus {
    pub fn spawn(
        committee: Committee,
        vertex_receiver: Receiver<Vertex>,
        gc_service: GarbageCollector
        // ordered_vertex_timestamps_sender: Sender<(Vertex, HashMap<Round, BTreeSet<Timestamp>>)>,
        // gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                vertex_receiver,
                // ordered_vertex_timestamps_sender,
                state: State::new(Vertex::genesis(committee.get_nodes_keys())),
                // gc_message_receiver,
                gc_service
            }.run().await;
        });
    }

    async fn run(&mut self) {
        // Listen to incoming vertices.
        while let Some(vertex) = self.vertex_receiver.recv().await {
            let round = vertex.round();
            debug!("Consensus received a vertex {} for round {}", vertex.encoded_hash(), round);

            // Add the new vertex to the local storage.
            self.state.insert_vertex(vertex);

            // Try to order the dag to commit. Start from the previous round and check if it is a leader round.
            let leader_round = round - 1;

            // We only elect leaders for even round numbers.
            if leader_round % WAVE != 0 || leader_round < WAVE {
                continue;
            }

            // Get the vertex's digest of the leader. If we already ordered this leader, there is nothing to do.
            if leader_round > self.state.last_committed_round {
                debug!("Start to elect leader for round {}", leader_round);
                let leader = match self.leader(leader_round) {
                    Some(x) => {
                        debug!("Found a leader {} for the round {}", x.encoded_owner(), leader_round);
                        x
                    }
                    None => {
                        warn!("No leader found in round {}. Skipping the ordering of vertices...", leader_round);
                        debug!("DAG: \n{}", self.state);
                        continue;
                    }
                };

                // Check if the leader has f+1 support from its children (ie. round r-1).
                // If it is the case, we can commit the leader. But first, we need to recursively go back to
                // the last committed leader, and commit all preceding leaders in the right order. Committing
                // a leader block means committing all its dependencies.
                if self.state.get_votes_for_vertex(&leader.hash(), &round) < self.committee.validity_threshold() {
                    warn!("Leader {} does not have enough support", leader.encoded_hash());
                    continue;
                }

                // Get an ordered list of past leaders that are linked to the current leader.
                debug!("Leader {} has enough support", leader.encoded_hash());
                for l in self.order_leaders(&leader).iter().rev() {
                    // Order vertices starting from the oldest leader
                    self.order_dag(l);
                    //TODO: maybe trigger it once for the last leader?
                    self.notify_gc(&l).await;
                }
                debug!("Vertices has been ordered from round {}. Current DAG:\n {}\n\
                Last ordered round is {}", round, self.state, self.state.last_committed_round);

            }
        }
    }

    /// Returns the vertex (and the vertex's digest) originated by the leader of the
    /// specified round (if any).
    fn leader(&self, round: Round) -> Option<&Vertex> {
        // At this stage, we are guaranteed to have 2f+1 vertices from round r (which is enough to
        // compute the coin). We currently just use round-robin.

        // Elect the leader.
        // let leader = self.committee.leader(coin);

        // self.state.get_vertex(&leader, &round)
        self.state.get_vertex_leader(&round)
    }

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(&self, leader: &Vertex) -> Vec<Vertex> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (self.state.last_committed_round + 2..leader.round()).rev().step_by(2)
        {
            // Get the vertex proposed by the previous leader.
            let prev_leader = match self.leader(r) {
                Some(x) => {
                    debug!("Found an uncommitted leader {} in the round {}", x.encoded_owner(), r);
                    x
                }
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.state.is_strongly_connected(leader, prev_leader) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            } else {
                debug!("Leaders {} and {} are not strongly connected", leader.encoded_owner(), prev_leader.encoded_owner());
            }
        }
        to_commit
    }

    fn order_dag(&mut self, leader: &Vertex) {
        debug!("Processing sub-dag of {:?}", leader);
        let mut buffer = vec![leader.clone()];

        while let Some(v) = buffer.pop() {
            let parents_round = v.round() - 1;
            if parents_round > self.state.last_committed_round {
                debug!("Ordering vertices of leader: {:?} for its parent round {}", v, parents_round);

                for (parent, _) in v.parents() {
                    if let Some(vertex) = self.state.set_vertex_as_delivered(parent, &parents_round) {
                        buffer.push(vertex);
                    }
                }
            }
        }
    }

    async fn notify_gc(&mut self, leader: &Vertex) {
        // Send vertex created timestamps for each round to GC.
        let timings = self.state.get_timings_before_round(leader.round());

        if let Some(gc_round) = self.gc_service.run(leader, timings) {
            self.state.clean_before_round(&gc_round);
        }

        /*if self.ordered_vertex_timestamps_sender.send((leader.clone(), timings)).await.is_ok() {
            debug!("Send ordered vertices to GC");
            match self.gc_message_receiver.recv().await {
                Ok(r) => {
                    debug!("Clean all vertices in the DAG before round {}", r);
                    self.state.clean_before_round(&r);
                }
                Err(e) => error!("Failed to receive the GC round: {:?}", e)
            }
        }*/
    }
}
