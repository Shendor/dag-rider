use std::collections::{BTreeSet, HashMap};
use log::info;
use tokio::sync::broadcast::Sender;
use model::{Round, Timestamp};
use model::vertex::Vertex;

/// Everything older than 3 sec must be cleaned up.
const GC_DELTA_TIME: Timestamp = 2000;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// A broadcast sender which notifies all participants about a garbage collected round.
    gc_round_notifier: Sender<Round>,
    /// The latest round which was collected.
    gc_round: Round,
}

impl GarbageCollector {
    pub fn new(gc_round_notifier: Sender<Round>) -> Self {
        Self {
            gc_round: 0,
            gc_round_notifier,
        }
    }

    pub fn run(&mut self, leader: &Vertex, timestamps_per_round: HashMap<Round, BTreeSet<Timestamp>>) -> Option<Round> {
        // Consensus sends a leader vertex and timestamps of its ordered vertices per round,
        // when it triggers ordering of vertices.
        let parents_timestamp = leader.parents().iter().map(|(_, (_, timestamp))| *timestamp).collect::<BTreeSet<Timestamp>>();
        let leader_ts = Self::median_timestamp(&parents_timestamp);

        let last_gc_round = self.gc_round;
        let round = leader.round();
        // If the leader's round is higher then recently gc round,
        // then check the previous rounds from it if they can be GC.
        if round > self.gc_round {
            let mut r = self.gc_round + 1;

            // Go in loop, starting from the last GC round, until the leader's parents round
            // and compare their median timestamps (created time).
            while r < round - 1 {
                let round_ts = match timestamps_per_round.get(&r) {
                    Some(timestamps) => Self::median_timestamp(timestamps),
                    None => leader_ts
                };

                if leader_ts > round_ts && leader_ts - round_ts > GC_DELTA_TIME {
                    self.gc_round = r;
                }
                r += 1;
            }
        }

        if last_gc_round != self.gc_round {
            self.notify_gc_round(last_gc_round, leader.encoded_owner());
            return Some(self.gc_round)
        }
        return None
    }

    fn notify_gc_round(&self, last_gc_round: Round, leader_hash: String) {
        info!("GC notifies about the collected rounds from {} to {} for the leader '{}'",
            last_gc_round, self.gc_round, leader_hash);
        let _ = self.gc_round_notifier.send(self.gc_round);
    }

    fn median_timestamp(timestamps: &BTreeSet<Timestamp>) -> Timestamp {
        let mut result = 0;
        let size = timestamps.len();
        let mut iter = timestamps.iter();
        for _ in 0..size / 2 {
            result = *iter.next().unwrap()
        }
        result
    }
}
