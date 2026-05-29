# Extending pathlockd

## Add a new primitive (RPC)

1. **Proto** (`proto/pathlockd.proto`): add the request/response messages and the
   `rpc` to the `PathLock` service. Keep enum values prefixed with the enum name
   (`FOO_STATUS_OK`) so prost strips the prefix to clean variants.
2. **Engine** (`src/engine.rs`): add `pub async fn foo(client, args) ->
   Result<FooOutcome>` wrapping the logic in `txn_retry!(client, serialize, tx
   => foo_inner(&mut tx, …).await)`. Choose `serialize = true` only if it mutates
   more than one key and must be ordered against other multi-key mutations.
   Encode logical results as a value enum, never as `Err`.
3. **Service** (`src/service.rs`): implement the trait method, mapping proto ⇄
   engine and publishing any events.
4. **Test** (`tests/engine_integration.rs`): assert the outcome value.
5. **Client** (`pathlockd-nodejs-client`): copy the updated `.proto` into the
   client package (`proto/`), add a typed wrapper method, rebuild. **The bundled
   proto must stay in sync** — a stale client proto silently drops new fields.

## Touching the data model

- Keys are defined by the `*_key` builders in `store.rs`. New per-owner or
  per-path data should follow the `fslock:<kind>:<...>` convention so GC/flush
  (range `fslock:`) cover it, and so it is isolated from `MUTEX_KEY`
  (`pathlockd:__serialize__`, deliberately outside that range).
- New values extend the `Stored` enum. Anything that should expire needs an
  `exp` and must be read through the expiry-aware helpers.

## Concurrency rules of thumb

- If a primitive's correctness depends on the *combined* state of several keys
  (e.g. "no conflicting lock anywhere in this subtree"), it must be `serialize =
  true` so it can't interleave with another such mutation.
- Single-key, self-contained operations can be `serialize = false`.
- Never hold results across transactions; re-read inside the transaction.

## Events

- Publish an event only at the point ownership actually changes, and only via
  the broadcaster so the per-owner filter and peer fan-out apply.
- Remember subscriptions are per-owner: only that owner will ever see the event.
  Cross-owner coordination must go through state the other side can poll.

## Gotchas

- `get_ancestors` is byte/`/`-based and assumes normalized paths
  (`handler:/a/b`, no trailing slash). Normalize on the client.
- Fencing tokens must stay monotonic; never write a lower fence for a path.
- Keep the `PathLockDebug` surface test-only and gated.
