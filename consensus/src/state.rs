use std::collections::{HashMap, HashSet};
use model::{MIN_QUORUM, Round};
use model::staker::NodePublicKey;
use model::vertex::{Vertex, VertexHash};
use crate::dag::Dag;

pub struct State {
    pub last_committed_round: Round,
    pub delivered_vertices: HashSet<VertexHash>,
    pub dag: Dag,
}

impl State {
    pub fn new(genesis_vertices: Vec<Vertex>) -> Self {
        let genesis = genesis_vertices.clone()
            .iter()
            .map(|x| (x.owner(), x.clone()))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            delivered_vertices: genesis.iter().map(|(h, v)| v.hash()).collect(),
            dag: Dag::new(genesis_vertices.clone(), MIN_QUORUM),
        }
    }

    pub fn set_vertex_as_delivered(&mut self, vertex: VertexHash) {
        self.delivered_vertices.insert(vertex);
    }

}