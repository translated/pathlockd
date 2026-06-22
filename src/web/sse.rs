//! Event delivery over HTTP via Server-Sent Events. The stream reads from the
//! per-owner [`EventLog`](super::eventlog::EventLog), so it shares the gRPC
//! broadcaster subscription and exposes resumable, monotonically-numbered ids.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};

use super::eventlog::StoredEvent;
use super::state::AppState;

/// How long the SSE loop blocks between event drains before looping to let the
/// keep-alive comment flush.
const SSE_TICK: Duration = Duration::from_secs(15);

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/events/sse", get(sse))
}

#[derive(Deserialize)]
struct SseQuery {
    #[serde(default)]
    owner_id: String,
    /// Resume after this id. The `Last-Event-ID` header takes precedence so a
    /// reconnecting `EventSource` resumes automatically.
    #[serde(default)]
    after: Option<u64>,
}

/// `GET /v1/events/sse?owner_id=…` — a `text/event-stream` of this owner's
/// lifecycle events. Each frame carries the monotonic id as the SSE `id:` field
/// so the browser resends it via `Last-Event-ID` on reconnect.
async fn sse(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SseQuery>,
) -> Response {
    if q.owner_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "owner_id is required").into_response();
    }
    let attach = st.log.attach(&q.owner_id);
    // Cursor precedence: Last-Event-ID header, then ?after=, else "from now"
    // (only future events) so a fresh stream is not flooded with history.
    let start = last_event_id(&headers)
        .or(q.after)
        .unwrap_or_else(|| attach.log().last_id());

    let stream = async_stream::stream! {
        let log = attach.log();
        let mut cursor = start;
        loop {
            // Register the wakeup *before* draining: `push` wakes via
            // notify_waiters (no stored permit), so an event appended between
            // the drain and the wait would otherwise stall until the tick.
            let wakeup = log.prepare_wait();
            for ev in log.since(cursor) {
                cursor = ev.id;
                yield Ok::<_, Infallible>(sse_frame(&ev));
            }
            // Block until a new event arrives (or the tick elapses so the
            // keep-alive can flush and we notice client disconnects).
            wakeup.wait(SSE_TICK).await;
        }
    };

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(SSE_TICK).text(":keep-alive"))
        .into_response()
}

fn sse_frame(ev: &StoredEvent) -> SseEvent {
    // json_data only fails if the value cannot serialize; EventView always can.
    SseEvent::default()
        .id(ev.id.to_string())
        .json_data(EventView::from(ev))
        .unwrap_or_else(|_| SseEvent::default().comment("serialize error"))
}

/// A retained event rendered for JSON/SSE: the resumable id plus the event type
/// as a stable string and the owner. Field names are camelCase to match the
/// proto3-JSON convention the message endpoints use.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventView {
    id: u64,
    #[serde(rename = "type")]
    kind: &'static str,
    owner_id: String,
}

impl From<&StoredEvent> for EventView {
    fn from(e: &StoredEvent) -> Self {
        EventView {
            id: e.id,
            kind: event_type_name(e.event.r#type),
            owner_id: e.event.owner_id.clone(),
        }
    }
}

fn event_type_name(t: i32) -> &'static str {
    match t {
        1 => "killed",
        2 => "revoke",
        3 => "grant",
        _ => "unspecified",
    }
}

fn last_event_id(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok())
}
