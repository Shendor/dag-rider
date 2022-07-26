use std::collections::{BTreeMap, HashMap};
use std::fmt::format;
use std::hash::Hash;
use model::Round;
use model::staker::NodePublicKey;
use model::vertex::{Vertex, VertexHash};

pub struct Dag {
    pub graph: HashMap<Round, HashMap<NodePublicKey, Vertex>>,
    min_quorum: u32,
}

impl Dag {
    pub fn new(root: Vec<Vertex>, min_quorum: u32) -> Self {
        let genesis = root
            .iter()
            .map(|v| (v.owner(), v.clone()))
            .collect::<HashMap<_, _>>();
        Dag {
            graph: [(0, genesis)].iter().cloned().collect(),
            min_quorum,
        }
    }

    pub fn insert_vertex(&mut self, vertex: Vertex) {
        self.graph
            .entry(vertex.round())
            .or_insert_with(HashMap::new)
            .insert(vertex.owner(), vertex);
    }

    pub fn is_quorum_reached_for_round(&self, round: &Round) -> bool {
        match self.graph.get(round) {
            Some(v) => v.len() as u32 >= self.min_quorum,
            None => false
        }
    }

    pub fn is_linked_with_others_in_round(&self, vertex: &Vertex, round: Round) -> bool {
        let mut weight = 0;
        for v in self.graph.get(&round).unwrap().values().iter() {
            if self.is_strongly_linked(vertex, v) {
                weight += 1;
            }
        }
        weight >= self.min_quorum
    }

    pub fn is_strongly_linked(&self, v1: &Vertex, v2: &Vertex) -> bool {
        self.is_linked_internal(v1, v2, |v: &Vertex| -> &BTreeMap<VertexHash, Round> { v.get_strong_parents() })
    }

    pub fn is_linked(&self, v1: &Vertex, v2: &Vertex) -> bool {
        self.is_linked_internal(v1, v2, |v: &Vertex| -> &BTreeMap<VertexHash, Round> { v.get_all_parents() })
    }

    fn is_linked_internal(&self, v1: &Vertex, v2: &Vertex, get_parents: fn(&Vertex) -> &BTreeMap<VertexHash, Round>) -> bool {
        if v1.round() > v2.round() {
            let mut vertex_stack = vec![v1];
            while !vertex_stack.is_empty() {
                let vertex = vertex_stack.pop().unwrap();
                for (parent, round) in get_parents(vertex) {
                    if *parent == v2.hash() {
                        return true;
                    } else if *round > v2.Round {
                        vertex_stack.push(to)
                    }
                }
            }
        }
        false
    }
}