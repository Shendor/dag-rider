mod dag;
mod state;
mod consensus2;

use log::{debug, log_enabled, warn};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};
use model::{Round, Wave};
use model::staker::NodePublicKey;
use model::vertex::Vertex;
use crate::state::State;

const MAX_WAVE: Wave = 4;

pub struct Consensus {
    vertex_receiver: Receiver<Vertex>,
    vertex_output_sender: Sender<Vertex>,
    decided_wave: Wave,
    state: State,
}

impl Consensus {
    pub fn spawn(
        nodes: Vec<NodePublicKey>,
        vertex_receiver: Receiver<Vertex>,
        vertex_output_sender: Sender<Vertex>,
    ) {
        tokio::spawn(async move {
            Self {
                vertex_receiver,
                vertex_output_sender,
                decided_wave: 0,
                state: State::new(Vertex::genesis(nodes)),
            }.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some(vertex) = self.vertex_receiver.recv().await {
            debug!("Processing {}", vertex);
            let round = vertex.round();

            self.state.dag.insert_vertex(vertex);

            if round <= self.state.last_committed_round && self.state.dag.is_quorum_reached_for_round(round) {
                if Self::is_last_round_in_wave(round) {
                    let ordered_vertices = self.get_ordered_vertices(round / MAX_WAVE);

                    for vertex in ordered_vertices {
                        self.vertex_output_sender
                            .send(certificate.clone())
                            .await
                            .expect("Failed to output vertex");

                        self.vertex_output_sender.send(vertex).await;
                    }
                }
                // when quorum for the round reached, then go to the next round
                self.state.last_committed_round += 1;
            }
        }
    }

    fn get_ordered_vertices(&mut self, wave: Wave) -> Vec<Vertex> {
        if let Some(leader) = self.get_wave_vertex_leader(wave) {
            // we need to make sure that if one correct process commits the wave
            // vertex leader 𝑣, then all the other correct processes will commit 𝑣
            // later. To this end, we use standard quorum intersection. Process 𝑝𝑖
            // commits the wave 𝑤 vertex leader 𝑣 if:
            if self.state.dag.is_linked_with_others_in_round(leader, self.get_round_for_wave(wave, MAX_WAVE)) {
                let mut leaders_to_commit = self.get_leaders_to_commit(wave - 1, leader);
                self.decided_wave = wave;

                // go through the un-committed leaders starting from the oldest one
                self.order_vertices(&mut leaders_to_commit)
            }
        }
        vec![]
    }

    fn get_leaders_to_commit(&self, from_wave: Wave, current_leader: &Vertex) -> Vec<Vertex> {
        let mut to_commit = vec![leader.clone()];
        let mut current_leader = current_leader;

        // Go for each wave up until decided_wave and find which leaders we need to commit
        for wave in (self.decided_wave + 1..from_wave).rev()
        {
            // Get the vertex proposed in the previous wave.
            if let Some(prev_leader) = self.get_wave_vertex_leader(wave) {
                // if no strong link between leaders then skip for this wave
                // and maybe next time there will be a strong link
                if self.state.dag.is_strongly_linked(current_leader, prev_leader) {
                    to_commit.push(prev_leader.clone());
                    current_leader = prev_leader;
                }
            }
        }
        to_commit
    }

    fn order_vertices(&mut self, leaders: &mut Vec<Vertex>) -> Vec<Vertex> {
        let mut delivered_vertices = Vec::new();

        // go from the oldest leader to the newest by taking items from the tail
        while let Some(leader) = leaders.pop() {
            debug!("Start ordering vertices from the leader: {:?}", leader);

            for (round, vertices) in self.state.dag.graph {
                if round > 0 {
                    for vertex in vertices.values() {
                        if !self.state.delivered_vertices.contains(&vertex.hash()) && self.state.dag.is_linked(&leader, vertex) {
                            delivered_vertices.push(vertex.clone());
                            self.state.set_vertex_as_delivered(vertex.hash());
                        }
                    }
                }
            }
        }

        delivered_vertices
    }

    fn get_wave_vertex_leader(&self, wave: Wave) -> Option<&Vertex> {
        //TODO: choose leader from the committee
        let leader = NodePublicKey::default();
        // leader is elected at the first round of the wave
        let first_round_of_wave = self.get_round_for_wave(wave, 1);
        self.state.dag.graph.get(&first_round_of_wave).map(|x| x.get(&leader)).flatten()
    }

    fn get_round_for_wave(&self, wave: Wave, round: Round) -> Round {
        (MAX_WAVE * (wave - 1) + round) as Round
    }

    fn is_last_round_in_wave(round: Round) -> bool {
        round % MAX_WAVE == 0
    }
}
