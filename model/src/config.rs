#[derive(Clone)]
pub struct Parameters {
    /// The preferred header size. The vertex coordinator creates a new header when it has enough parents and
    /// enough batches' digests to reach `header_size`. Denominated in bytes.
    pub header_size: usize,
    /// The maximum delay that the vertex coordinator waits between generating two headers, even if the header
    /// did not reach `max_header_size`. Denominated in ms.
    pub max_header_delay: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,
}

impl Parameters {
    pub fn new() -> Self {
        Self {
            header_size: 1000,
            max_header_delay: 100,
            sync_retry_delay: 3000,
            sync_retry_nodes: 3,
            batch_size: 100000,
            max_batch_delay: 100,
        }
    }
}