//! A minimal, safe replacement for the `asynk_strim::try_stream_fn` the two
//! subscription resolvers (`event_subscription`, `projection_subscription`)
//! build their live feeds with - keeps that dependency out of the tree,
//! using only `async-graphql`'s own re-exported `futures_util`.
//!
//! The body is an ordinary `async` block handed a [`TryYielder`]; each
//! `yield_ok`/`yield_error` suspends it until the stream's consumer has
//! taken the value, so the body runs exactly as lazily as the `asynk_strim`
//! version did. The body future is owned by the returned stream and dropped
//! with it - releasing, for instance, the subscription-slot guard the body
//! holds - rather than run ahead on a spawned task.
//!
//! How the suspension works: [`TryYielder::yield_item`] awaits a
//! `poll_fn` that stashes the item on its first poll and returns
//! `Pending`, then returns `Ready` on the second. The stream's
//! `poll_next` polls the body, then drains whatever the yielder stashed:
//! the stashed item is what `poll_next` returns, and the *next* poll
//! resumes the body past its yield. No waker trickery is needed, because
//! the consumer necessarily polls again after every item it is handed.

use async_graphql::futures_util::{future::poll_fn, Stream};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Builds a stream of `Result<Ok, Err>` from a generator body, mirroring
/// `asynk_strim::try_stream_fn`: `body` is handed a [`TryYielder`] and
/// yields through it; a body that returns `Err` yields that error last and
/// then ends, exactly as `try_stream_fn` did.
pub fn try_stream_fn<F, Fut, Ok, Err>(body: F) -> impl Stream<Item = Result<Ok, Err>>
where
    F: FnOnce(TryYielder<Ok, Err>) -> Fut,
    Fut: Future<Output = Result<(), Err>>,
{
    let slot = Arc::new(Mutex::new(None));
    let yielder = TryYielder {
        slot: Arc::clone(&slot),
    };
    let body = body(yielder.clone());
    let body = async move {
        if let Err(err) = body.await {
            yielder.yield_error(err).await;
        }
    };
    Generator {
        body: Some(Box::pin(body)),
        slot,
    }
}

/// The generator body's own handle onto its stream - see [`try_stream_fn`].
pub struct TryYielder<Ok, Err> {
    slot: Arc<Mutex<Option<Result<Ok, Err>>>>,
}

impl<Ok, Err> Clone for TryYielder<Ok, Err> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<Ok, Err> TryYielder<Ok, Err> {
    /// Yields one successful item; the body stays suspended until the
    /// consumer has taken it.
    pub async fn yield_ok(&self, ok: Ok) {
        self.yield_item(Ok(ok)).await;
    }

    /// Yields one error item; same suspension as [`Self::yield_ok`].
    pub async fn yield_error(&self, err: Err) {
        self.yield_item(Err(err)).await;
    }

    async fn yield_item(&self, item: Result<Ok, Err>) {
        let slot = Arc::clone(&self.slot);
        let mut pending = Some(item);
        poll_fn(move |_cx| match pending.take() {
            Some(item) => {
                *slot.lock().unwrap() = Some(item);
                Poll::Pending
            }
            None => Poll::Ready(()),
        })
        .await;
    }
}

/// The stream [`try_stream_fn`] returns - see its own doc comment.
struct Generator<Ok, Err, Fut> {
    body: Option<Pin<Box<Fut>>>,
    slot: Arc<Mutex<Option<Result<Ok, Err>>>>,
}

impl<Ok, Err, Fut: Future<Output = ()>> Stream for Generator<Ok, Err, Fut> {
    type Item = Result<Ok, Err>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(body) = this.body.as_mut() {
            if body.as_mut().poll(cx).is_ready() {
                this.body = None;
            }
        }
        if let Some(item) = this.slot.lock().unwrap().take() {
            Poll::Ready(Some(item))
        } else if this.body.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::try_stream_fn;
    use async_graphql::futures_util::{pin_mut, StreamExt};

    #[tokio::test]
    async fn yields_in_order_then_a_final_error() {
        let stream = try_stream_fn(|yielder| async move {
            yielder.yield_ok(1).await;
            yielder.yield_ok(2).await;
            yielder.yield_error("boom").await;
            Ok(())
        });
        pin_mut!(stream);
        assert_eq!(stream.next().await, Some(Ok(1)));
        assert_eq!(stream.next().await, Some(Ok(2)));
        assert_eq!(stream.next().await, Some(Err("boom")));
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn a_returned_error_is_yielded_last() {
        let stream = try_stream_fn(|yielder: super::TryYielder<i32, &str>| async move {
            yielder.yield_ok(1).await;
            Err("final")
        });
        pin_mut!(stream);
        assert_eq!(stream.next().await, Some(Ok(1)));
        assert_eq!(stream.next().await, Some(Err("final")));
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn an_empty_body_ends_immediately() {
        let stream = try_stream_fn(|_yielder: super::TryYielder<i32, &str>| async move { Ok(()) });
        pin_mut!(stream);
        assert_eq!(stream.next().await, None);
    }
}
