use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use log::debug;

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

    pub fn get_vertices(&self, round: &Round) -> BTreeMap<VertexHash, (Round, u128)> {
        match self.dag.get(round) {
            Some(v) => v.iter().map(|(h, v)| (*h, (v.round(), v.created_time()))).collect(),
            None => BTreeMap::default()
        }
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

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut vertex_ids = HashMap::new();
        for (r, vertices) in &self.dag {
            let mut line = format!("{}: ", r.to_string());

            let mut c = 1;
            for (hash, vertex) in vertices {
                vertex_ids.insert(hash, c);

                let mut parents_line = String::new();
                for (parent, (round, _)) in vertex.parents() {
                    if let Some(id) = vertex_ids.get(parent) {
                        parents_line.push_str(format!(" {}-{}", round, id).as_str());
                    }
                }

                if parents_line.is_empty() {
                    line.push_str(format!("(V{})", c).as_str());
                } else {
                    line.push_str(format!("(V{})[{} ]", c, parents_line).as_str());
                }
                if c < vertices.len() {
                    line.push_str(" --- ");
                }

                c += 1;
            }
            line.push_str("\n");
            write!(f, "{}", line);
        }
        Ok(())
    }
}