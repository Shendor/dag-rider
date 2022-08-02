use std::collections::{HashMap, HashSet};

use model::Round;
use model::vertex::{Vertex, VertexHash};

use crate::dag::Dag;

pub struct State {
    pub current_round: Round,
    pub delivered_vertices: HashSet<VertexHash>,
    pub dag: Dag,
}

impl State {
    pub fn new(genesis_vertices: Vec<Vertex>) -> Self {
        let min_quorum = (2 * genesis_vertices.len() / 3 + 1) as u32;
        let genesis = genesis_vertices.clone()
            .iter()
            .map(|x| (x.owner(), x.clone()))
            .collect::<HashMap<_, _>>();

        Self {
            current_round: 1,
            delivered_vertices: genesis.iter().map(|(_, v)| v.hash()).collect(),
            dag: Dag::new(genesis_vertices.clone(), min_quorum),
        }
    }

    pub fn set_vertex_as_delivered(&mut self, vertex_hash: VertexHash) {
        self.delivered_vertices.insert(vertex_hash);
    }

}