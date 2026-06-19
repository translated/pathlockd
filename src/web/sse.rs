//! Event delivery over HTTP: Server-Sent Events (modern clients) and a
//! long-poll fallback (legacy clients without `EventSource`). Both read from the
//! per-owner [`EventLog`](super::eventlog::EventLog) so they share one
//! broadcaster subscription and identical, resumable event ids.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::eventlog::StoredEvent;
use super::state::AppState;

/// How long the SSE loop blocks between event drains before looping to let the
/// keep-alive comment flush.
const SSE_TICK: Duration = Duration::from_secs(15);

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/events/sse", get(sse))
        .route("/v1/events/poll", get(poll))
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
        let mut cursor = start;
        loop {
            for ev in attach.log().since(cursor) {
                cursor = ev.id;
                yield Ok::<_, Infallible>(sse_frame(&ev));
            }
            // Block until a new event arrives (or the tick elapses so the
            // keep-alive can flush and we notice client disconnects).
            attach.log().wait(SSE_TICK).await;
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

#[derive(Deserialize)]
struct PollQuery {
    #[serde(default)]
    owner_id: String,
    /// Return events with id greater than this. Clients persist `last_id` from
    /// the previous response and pass it back here.
    #[serde(default)]
    after: u64,
}

#[derive(Serialize)]
struct PollResponse {
    events: Vec<EventView>,
    /// Highest id in this batch (or the request `after` when empty); the cursor
    /// for the next poll.
    last_id: u64,
}

/// `GET /v1/events/poll?owner_id=…&after=…` — long-poll fallback. Returns any
/// events newer than `after`; if none, blocks up to `web_poll_wait_ms` for one
/// to arrive, then returns (possibly empty) so the client can immediately poll
/// again.
async fn poll(State(st): State<AppState>, Query(q): Query<PollQuery>) -> Response {
    if q.owner_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "owner_id is required").into_response();
    }
    let attach = st.log.attach(&q.owner_id);
    let mut events = attach.log().since(q.after);
    if events.is_empty() {
        attach.log().wait(st.poll_wait).await;
        events = attach.log().since(q.after);
    }
    let last_id = events.last().map(|e| e.id).unwrap_or(q.after);
    let body = PollResponse {
        events: events.iter().map(EventView::from).collect(),
        last_id,
    };
    Json(body).into_response()
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
