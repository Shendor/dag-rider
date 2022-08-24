use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};

use model::Round;
use model::vertex::{Vertex, VertexHash};

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<VertexHash, Vertex>>;

/// The state that needs to be persisted for crash-recovery.
pub struct State {
    /// The last committed round.
    pub last_committed_round: Round,
    pub delivered_vertices: HashSet<VertexHash>,
    /// Keeps the latest committed vertex (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    pub fn new(genesis: Vec<Vertex>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|v| (v.hash(), v))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            delivered_vertices: HashSet::new(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    pub fn set_vertex_as_delivered(&mut self, vertex_hash: &VertexHash, round: &Round) -> Option<Vertex> {
        if let Some(vertex) = self.dag
                                  .get(&round)
                                  .map(|vertices| vertices.get(vertex_hash))
                                  .flatten()
        {
            self.delivered_vertices.insert(vertex.hash());
            self.last_committed_round = max(self.last_committed_round, vertex.round());
            return Some(vertex.clone());
        }
        return None;
    }

    pub fn insert_vertex(&mut self, vertex: Vertex) {
        self.dag
            .entry(vertex.round())
            .or_insert_with(|| HashMap::new())
            .insert(vertex.hash(), vertex);
    }

    pub fn contains_vertices(&self, vertices: &BTreeMap<VertexHash, (Round, u128)>) -> bool {
        vertices.iter()
                .all(|(vertex_hash,
                          (round, _))| self.dag.get(round)
                                           .map_or_else(|| false, |v| v.contains_key(vertex_hash)))
    }

    pub fn get_vertices(&self, round: &Round) -> BTreeMap<VertexHash, (Round, u128)> {
        match self.dag.get(round) {
            Some(v) => v.iter().map(|(h, v)| (*h, (v.round(), v.created_time()))).collect(),
            None => BTreeMap::default()
        }
    }

    fn find_vertex_from_hash(&self, vertex_hash: &VertexHash, round: &Round) -> Option<&Vertex> {
        self.dag
            .get(round)
            .map(|vertices| vertices.get(vertex_hash))
            .flatten()
    }

    pub fn get_votes_for_vertex(&self, vertex_hash: &VertexHash, round: &Round) -> usize {
        self.dag
            .get(round)
            .map_or_else(|| 0,
                         |v| v.values()
                              .filter(|v| v.parents().contains_key(vertex_hash))
                              .count())
    }

    pub fn is_strongly_connected(&self, leader: &Vertex, previous_leader: &Vertex) -> bool {
        let mut parents = HashMap::new();
        parents.insert(leader.hash(), leader);
        // go backwards from parent round of the `leader` vertex
        // till the round of the previous leader,
        // collecting vertices with strong links.
        for r in (previous_leader.round()..leader.round()).rev() {
            parents = self.dag
                          .get(&r)
                          .expect("We should have the whole history by now")
                          .iter()
                          .filter(|(h, _)| parents.iter().any(|(_, p)| p.get_strong_parents().contains_key(*h)))
                          .map(|(h, v)| (*h, v))
                          .collect::<HashMap<VertexHash, &Vertex>>();
        }
        // check if the last round of parent vertices contains the `previous_leader`
        parents.contains_key(&previous_leader.hash())
    }


    pub fn get_vertex(&self, vertex_hash: &VertexHash, round: &Round) -> Option<&Vertex> {
        self.dag.get(round).map_or_else(|| None, |v| v.get(vertex_hash))
    }
}
