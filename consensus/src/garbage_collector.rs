use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc::{Receiver, Sender};
use model::Round;
use model::vertex::Vertex;

const GC_DELTA_TIME: u128 = 3 * 100000;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// Receives the leader and timings of ordered vertices for this leader.
    /// The timings are grouped by rounds so GC can calculate median time per round.
    ordered_vertex_timestamps_receiver: Receiver<(Vertex, BTreeMap<Round, BTreeSet<u128>>)>,
    gc_round_sender: Sender<Round>,
    gc_round: Round,
}

impl GarbageCollector {
    pub fn spawn(
        ordered_vertex_timestamps_receiver: Receiver<(Vertex, BTreeMap<Round, BTreeSet<u128>>)>,
        gc_round_sender: Sender<Round>
    ) {
        tokio::spawn(async move {
            Self {
                gc_round: 0,
                ordered_vertex_timestamps_receiver,
                gc_round_sender
            }.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some((leader, ordered_vertex_timestamps)) = self.ordered_vertex_timestamps_receiver.recv().await {

            let leader_ts = Self::median_timestamp(&leader.parents().iter().map(|(_, (_, timestamp))| *timestamp).collect::<BTreeSet<u128>>());

            let round = leader.round();
            if round > self.gc_round {
                let mut r = self.gc_round + 1;

                while r < round - 1 {
                    let round_ts = match ordered_vertex_timestamps.get(&r) {
                        Some(timestamps) => Self::median_timestamp(timestamps),
                        None => leader_ts
                    };

                    if leader_ts - round_ts > GC_DELTA_TIME {
                        self.gc_round = r;
                        self.notify_gc_round();
                    }
                    r += 1;
                }
            }
        }
    }

    fn median_timestamp(timestamps: &BTreeSet<u128>) -> u128 {
        let mut result = 0;
        let size = timestamps.len();
        let mut iter = timestamps.iter();
        for _ in 0..size / 2 {
            result = *iter.next().unwrap()
        }
        result
    }

    fn notify_gc_round(&mut self) {
        self.gc_round_sender.send(self.gc_round);
    }
}
