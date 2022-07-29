use std::collections::HashSet;
use log::{debug};
use tokio::sync::mpsc::{Receiver, Sender};

use model::{Round, Wave};
use model::block::Block;
use model::committee::{Committee, Id};
use model::vertex::{Vertex, VertexHash};

use crate::state::State;

mod dag;
mod state;

const MAX_WAVE: Wave = 4;

pub struct Consensus {
    node_id: Id,
    committee: Committee,
    decided_wave: Wave,
    state: State,
    delivered_vertices: HashSet<VertexHash>,
    buffer: Vec<Vertex>,
    blocks_receiver: Receiver<Block>,
    vertex_receiver: Receiver<Vertex>,
    vertex_output_sender: Sender<Vertex>,
    vertex_to_broadcast_sender: Sender<Vertex>,
}

impl Consensus {
    pub fn spawn(
        node_id: Id,
        committee: Committee,
        vertex_receiver: Receiver<Vertex>,
        vertex_to_broadcast_sender: Sender<Vertex>,
        vertex_output_sender: Sender<Vertex>,
        blocks_receiver: Receiver<Block>,
    ) {
        tokio::spawn(async move {
            let state = State::new(Vertex::genesis(committee.get_nodes_keys()));
            Self {
                node_id,
                committee,
                vertex_receiver,
                vertex_output_sender,
                vertex_to_broadcast_sender,
                decided_wave: 0,
                state,
                delivered_vertices: HashSet::new(),
                buffer: vec![],
                blocks_receiver,
            }.run().await;
        });
    }

    async fn run(&mut self) {
        loop {
            if let Some(vertex) = self.vertex_receiver.recv().await {
                debug!("Vertex received in consensus of node {}: {}", self.node_id, vertex);
                self.buffer.push(vertex);

                // Go through buffer and add vertex in the dag which meets the requirements
                // and remove from the buffer those added
                self.buffer.retain(|v| {
                    if v.round() <= self.state.current_round && self.state.dag.contains_vertices(v.parents()) {
                        self.state.dag.insert_vertex(v.clone());
                        false
                    } else {
                        true
                    }
                })
            }

            if self.state.dag.is_quorum_reached_for_round(&(self.state.current_round)) {
                if Self::is_last_round_in_wave(self.state.current_round) {
                    let ordered_vertices = self.get_ordered_vertices(self.state.current_round / MAX_WAVE);

                    for vertex in ordered_vertices {
                        self.vertex_output_sender
                            .send(vertex.clone())
                            .await
                            .expect("Failed to output vertex");

                        self.vertex_output_sender.send(vertex).await.unwrap();
                    }
                }
                // when quorum for the round reached, then go to the next round
                self.state.current_round += 1;
                let new_vertex = self.create_new_vertex(self.state.current_round).await.unwrap();
                self.vertex_to_broadcast_sender.send(new_vertex).await.unwrap();
            }
        }
    }

    async fn create_new_vertex(&mut self, round: Round) -> Option<Vertex> {
        if let Some(block) = self.blocks_receiver.recv().await {
            let parents = self.state.dag.get_vertices(&round);
            let mut vertex = Vertex::new(
                self.committee.get_node_key(self.node_id).unwrap(),
                round,
                block,
                parents,
            );
            self.set_weak_edges(&mut vertex, round);

            return Some(vertex)
        }
        return None
    }

    fn set_weak_edges(&self, vertex: &mut Vertex, round: Round) {
        for r in (round - 2..1).rev() {
            for (_, v) in self.state.dag.graph.get(&r).unwrap() {
                if !self.state.dag.is_linked(&vertex, v) {
                    vertex.add_parent(v.hash(), r)
                }
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
                return self.order_vertices(&mut leaders_to_commit);
            }
        }
        return vec![];
    }

    fn get_leaders_to_commit(&self, from_wave: Wave, current_leader: &Vertex) -> Vec<Vertex> {
        let mut to_commit = vec![current_leader.clone()];
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
        let mut ordered_vertices = Vec::new();

        // go from the oldest leader to the newest by taking items from the tail
        while let Some(leader) = leaders.pop() {
            debug!("Start ordering vertices from the leader: {:?}", leader);

            for (round, vertices) in &self.state.dag.graph {
                if *round > 0 {
                    for vertex in vertices.values() {
                        let vertex_hash = vertex.hash();
                        if !self.delivered_vertices.contains(&vertex_hash) && self.state.dag.is_linked(&leader, vertex) {
                            ordered_vertices.push(vertex.clone());
                            self.delivered_vertices.insert(vertex_hash);
                        }
                    }
                }
            }
        }

        ordered_vertices
    }

    fn get_wave_vertex_leader(&self, wave: Wave) -> Option<&Vertex> {
        let first_round_of_wave = self.get_round_for_wave(wave, 1);
        let coin = first_round_of_wave;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.get_nodes_keys();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // leader is elected at the first round of the wave
        self.state.dag.graph.get(&first_round_of_wave).map(|x| x.get(&leader)).flatten()
    }

    fn get_round_for_wave(&self, wave: Wave, round: Round) -> Round {
        (MAX_WAVE * (wave - 1) + round) as Round
    }

    fn is_last_round_in_wave(round: Round) -> bool {
        round % MAX_WAVE == 0
    }
}
