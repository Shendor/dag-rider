use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use crate::CancelHandler;

pub async fn wait(min_responses: usize, handlers: Vec<CancelHandler>) -> bool {
    let mut future_responses: FuturesUnordered<_> = handlers
        .into_iter()
        .map(|handler| async {
            let _ = handler.await;
            true
        })
        .collect();

    let mut counter = 1;
    while let Some(true) = future_responses.next().await {
        counter += 1;
        if counter >= min_responses {
            return true;
        }
    }
    return false
}
