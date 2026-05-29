/// Run a transactional body against TiKV with bounded retry on transient
/// errors (write conflicts, region churn).
///
/// Usage:
/// ```ignore
/// let outcome = txn_retry!(client, /*serialize=*/true, tx => {
///     acquire_inner(&mut tx, &args).await
/// })?;
/// ```
///
/// The body must evaluate to an `anyhow::Result<T>`; the *logical* outcomes of
/// a lock operation (conflict / lost / ok) are encoded in `T`, never as `Err`,
/// so they are committed normally. Only infrastructure errors trigger rollback
/// and retry.
///
/// `serialize` makes the transaction write the global serialization key, so any
/// two overlapping multi-key mutations collide at commit (optimistic
/// write-write conflict) and one retries with a fresh snapshot — giving
/// single-threaded atomicity cluster-wide without pessimistic-lock lifecycle
/// hazards. Single-key ops pass `false`.
#[macro_export]
macro_rules! txn_retry {
    ($client:expr, $serialize:expr, $tx:ident => $body:expr) => {{
        let mut __attempt: u32 = 0;
        loop {
            let mut $tx = $crate::store::Tx::begin($client, $serialize).await?;
            let __res: ::anyhow::Result<_> = $body;
            match __res {
                Ok(__v) => match $tx.commit().await {
                    Ok(()) => break Ok(__v),
                    Err(__e) => {
                        if __attempt < $crate::store::MAX_RETRY && $crate::store::is_retryable(&__e) {
                            __attempt += 1;
                            $crate::store::backoff(__attempt).await;
                            continue;
                        }
                        break Err(__e);
                    }
                },
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
}
