/// Run a transactional body against TiKV with bounded retry on transient
/// errors (write conflicts, region churn), including transaction begin errors.
///
/// Usage:
/// ```ignore
/// // Always commit on a logical Ok:
/// let outcome = txn_retry!(client, tx => { release_inner(&mut tx, owner).await })?;
///
/// // Commit only when the outcome actually mutated state; roll back otherwise:
/// let outcome = txn_retry!(client, commit_if: |o| matches!(o, AcquireOutcome::Ok),
///     tx => { acquire_inner(&mut tx, &args).await })?;
/// ```
///
/// The body must evaluate to an `anyhow::Result<T>`; the *logical* outcomes of
/// a lock operation (conflict / lost / ok) are encoded in `T`, never as `Err`,
/// so they are committed normally. Only infrastructure errors trigger rollback
/// and retry.
///
/// Serialization is opt-in *inside* the body: a multi-key mutation calls
/// `tx.serialize_handler(handler)` for every handler it touches, so two
/// mutations on the same handler collide at commit (optimistic write-write
/// conflict) and the loser retries with a fresh snapshot — per-handler
/// single-threaded atomicity without pessimistic-lock hazards. Single-key and
/// advisory operations simply never call it.
///
/// `commit_if` lets an operation skip the commit (and therefore the
/// serialization-key write) when the outcome performed no durable mutation —
/// e.g. an acquire that returns CONFLICT/LOST from its read-only validation
/// phase. Rolling those back avoids serializing failed attempts; a stale
/// negative only makes the client retry, which is safe.
#[macro_export]
macro_rules! txn_retry {
    ($client:expr, commit_if: $pred:expr, $tx:ident => $body:expr) => {{
        let mut __attempt: u32 = 0;
        loop {
            let mut $tx = match $crate::store::Tx::begin($client).await {
                Ok(__tx) => __tx,
                Err(__e) => {
                    if __attempt < $crate::store::MAX_RETRY
                        && $crate::store::is_retryable(&__e)
                    {
                        __attempt += 1;
                        $crate::store::backoff(__attempt).await;
                        continue;
                    }
                    break Err(__e);
                }
            };
            let __res: ::anyhow::Result<_> = $body;
            match __res {
                Ok(__v) => {
                    if !$pred(&__v) {
                        // No durable mutation in this outcome — roll back so we
                        // neither serialize nor touch storage.
                        let _ = $tx.rollback().await;
                        break Ok(__v);
                    }
                    match $tx.commit().await {
                        Ok(()) => break Ok(__v),
                        Err(__e) => {
                            if __attempt < $crate::store::MAX_RETRY
                                && $crate::store::is_retryable(&__e)
                            {
                                __attempt += 1;
                                $crate::store::backoff(__attempt).await;
                                continue;
                            }
                            break Err(__e);
                        }
                    }
                }
                Err(__e) => {
                    let _ = $tx.rollback().await;
                    if __attempt < $crate::store::MAX_RETRY && $crate::store::is_retryable(&__e) {
                        __attempt += 1;
                        $crate::store::backoff(__attempt).await;
                        continue;
                    }
                    break Err(__e);
                }
            }
        }
    }};
    ($client:expr, $tx:ident => $body:expr) => {{
        $crate::txn_retry!($client, commit_if: |_| true, $tx => $body)
    }};
}
