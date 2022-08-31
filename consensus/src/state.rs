use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use model::committee::NodePublicKey;

use model::{Round, Timestamp};
use model::vertex::{Vertex, VertexHash};

/// The representation of the DAG in memory.
type Dag = BTreeMap<Round, HashMap<VertexHash, Vertex>>;

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

    /// Mark vertex as delivered for the round
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

    /// Add vertex to the DAG
    pub fn insert_vertex(&mut self, vertex: Vertex) {
        self.dag
            .entry(vertex.round())
            .or_insert_with(|| HashMap::new())
            .insert(vertex.hash(), vertex);
    }

    /// Get created times of vertices, grouped by round.
    /// This data can be used to find out which rounds can be cleaned up in the DAG.
    pub fn get_timings_before_round(&self, round: Round) -> HashMap<Round, BTreeSet<Timestamp>> {
        let mut grouped_timestamps_per_round: HashMap<Round, BTreeSet<Timestamp>> = HashMap::new();
        self.dag.iter()
            .filter(|(r, _)| **r < round)
            .for_each(|(r, values)| {
                values.iter().for_each(|(_, v)| {
                    grouped_timestamps_per_round.entry(*r).or_insert(BTreeSet::new()).insert(v.created_time());
                });
            });
        grouped_timestamps_per_round
    }

    /// Get the number of children of the `vertex_hash`, which implies the number
    /// of the votes for the vertex.
    pub fn get_votes_for_vertex(&self, vertex_hash: &VertexHash, round: &Round) -> usize {
        self.dag
            .get(round)
            .map_or_else(|| 0,
                         |v| v.values()
                              .filter(|v| v.parents().contains_key(vertex_hash))
                              .count())
    }

    /// Verify if there is a path between 2 vertices via strong edges.
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
                          .filter(|(h, _)| parents.iter().any(|(_, p)| p.parents().contains_key(*h)))
                          .map(|(h, v)| (*h, v))
                          .collect::<HashMap<VertexHash, &Vertex>>();
        }
        // check if the last round of parent vertices contains the `previous_leader`
        parents.contains_key(&previous_leader.hash())
    }

    pub fn get_vertex(&self, vertex_owner: &NodePublicKey, round: &Round) -> Option<&Vertex> {
        //TODO: Find a quicker way to get a leader vertex
        self.dag.get(round).map_or_else(|| None, |vertices| vertices.values().find(|v| v.owner() == *vertex_owner))
    }

    pub fn get_vertex_leader(&self, round: &Round) -> Option<&Vertex> {
        self.dag.get(round).map_or_else(|| None, |vertices| vertices.values().next())
    }

    /// Clean all vertices from the beginning to the provided round.
    pub fn clean_before_round(&mut self, round: &Round) {
        self.dag.retain(|r, _| r > round)
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
                        // if link to the previous round, then skip displaying it
                        // otherwise it's a weak link then show the round number
                        // for the parent vertex.
                        if *r - *round == 1 {
                            parents_line.push_str(format!(" v{}", id).as_str());
                        } else {
                            parents_line.push_str(format!(" v{}({})", id, round).as_str());
                        }
                    }
                }

                let is_delivered_char = if self.delivered_vertices.contains(hash) { "*" } else { "" };
                let blocks_count_char =
                    if vertex.get_blocks().len() > 0 { format!("b={}", vertex.get_blocks().len()) } else { String::new() };
                if parents_line.is_empty() {
                    line.push_str(format!("| V{}({}) |", c, vertex.short_encoded_owner()).as_str());
                } else {
                    line.push_str(format!("| V{}{}({})[{} ] {} |",
                                          c, is_delivered_char, vertex.short_encoded_owner(), parents_line, blocks_count_char).as_str());
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