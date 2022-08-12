use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use model::committee::NodePublicKey;
use model::Round;
use model::vertex::{Vertex, VertexHash};

pub struct Dag {
    pub graph: BTreeMap<Round, HashMap<NodePublicKey, Vertex>>,
    min_quorum: u32,
}

impl Dag {
    pub fn new(root: Vec<Vertex>, min_quorum: u32) -> Self {
        let genesis = root
            .iter()
            .map(|v| (v.owner(), v.clone()))
            .collect::<HashMap<_, _>>();
        Dag {
            graph: [(1, genesis)].iter().cloned().collect(),
            min_quorum,
        }
    }

    pub fn insert_vertex(&mut self, vertex: Vertex) {
        self.graph
            .entry(vertex.round())
            .or_insert_with(HashMap::new)
            .insert(vertex.owner(), vertex);
    }

    pub fn contains_vertices(&self, vertices: &BTreeMap<VertexHash, Round>) -> bool {
        vertices.iter().all(|(vertex_hash, round)| {
            match self.graph.get(round) {
                Some(v) => v.values().any(|vertex| vertex.hash() == *vertex_hash),
                None => false
            }
        })
    }

    pub fn get_vertices(&self, round: &Round) -> BTreeMap<VertexHash, Round> {
        match self.graph.get(round) {
            Some(v) => v.iter().map(|(_, v)| { (v.hash(), v.round()) }).collect(),
            None => BTreeMap::default()
        }
    }

    pub fn is_quorum_reached_for_round(&self, round: &Round) -> bool {
        match self.graph.get(round) {
            Some(v) => v.len() as u32 >= self.min_quorum,
            None => false
        }
    }

    pub fn is_linked_with_others_in_round(&self, vertex: &Vertex, round: Round) -> bool {
        let mut weight = 0;
        for v in self.graph.get(&round).unwrap().values() {
            if self.is_strongly_linked(v, vertex) {
                weight += 1;
            }
        }
        weight >= self.min_quorum
    }

    pub fn is_strongly_linked(&self, newest: &Vertex, oldest: &Vertex) -> bool {
        self.is_linked_internal(newest, oldest, |v: &Vertex| -> BTreeMap<VertexHash, Round> { v.get_strong_parents() })
    }

    pub fn is_linked(&self, newest: &Vertex, oldest: &Vertex) -> bool {
        self.is_linked_internal(newest, oldest, |v: &Vertex| -> BTreeMap<VertexHash, Round> { v.get_all_parents() })
    }

    fn is_linked_internal(&self, newest: &Vertex, oldest: &Vertex, get_parents: fn(&Vertex) -> BTreeMap<VertexHash, Round>) -> bool {
        if newest.round() > oldest.round() {
            let mut vertex_stack = vec![newest];
            while !vertex_stack.is_empty() {
                let vertex = vertex_stack.pop().unwrap();
                for (parent, round) in get_parents(vertex) {
                    if parent == oldest.hash() {
                        return true;
                    } else if round > oldest.round() {
                        if let Some(parent_vertex) = self.get_vertex(parent, &round) {
                            vertex_stack.push(parent_vertex)
                        }
                    }
                }
            }
        }
        false
    }

    pub fn get_vertex(&self, vertex_hash: VertexHash, round: &Round) -> Option<&Vertex> {
        match self.graph.get(round) {
            Some(v) => v.values().find(|vertex| vertex.hash() == vertex_hash),
            None => None
        }
    }
}

impl Display for Dag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut vertex_ids = HashMap::new();
        for (r, vertices) in &self.graph {
            let mut line = format!("{}: ", r.to_string());

            let mut c = 1;
            for (_, vertex) in vertices {
                vertex_ids.insert(vertex.hash(), c);

                let mut parents_line = String::new();
                for (hash, round) in vertex.parents() {
                    if let Some(id) = vertex_ids.get(hash) {
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