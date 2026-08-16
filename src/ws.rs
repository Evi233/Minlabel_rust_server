use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/ws", get(ws_handler))
}

#[derive(Deserialize)]
struct WsQuery {
    user: String,
    room: String,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    if q.user.trim().is_empty() || q.room.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !state
        .db
        .room_exists(&q.room)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        return Err(StatusCode::NOT_FOUND);
    }
    let user = q.user.trim().to_string();
    let room = q.room.trim().to_string();
    Ok(ws.on_upgrade(move |socket| handle_socket(socket, room, user, state)))
}

async fn handle_socket(socket: WebSocket, room: String, user: String, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(64);

    state
        .clients
        .lock()
        .unwrap()
        .insert((room.clone(), user.clone()), tx.clone());

    let mut broadcast_rx = state.room_channel(&room).subscribe();
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(text) => {
                            if sender.send(Message::Text(text)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                msg = broadcast_rx.recv() => {
                    match msg {
                        Ok(text) => {
                            if sender.send(Message::Text(text)).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    let (done, total) = state.db.progress_for_room(&room).unwrap_or((0, 0));
    let _ = tx
        .send(json!({ "type": "progress", "done": done, "total": total }).to_string())
        .await;

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                handle_client_message(&state, &room, &user, &text, &tx).await;
            }
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Binary(_) => {}
        }
    }

    let released = state.release_all(&user);
    for id in released {
        state.broadcast_to(
            &room,
            &json!({ "type": "release", "user": user, "file_id": id }).to_string(),
        );
    }
    state.clients.lock().unwrap().remove(&(room, user));
    send_task.abort();
}

async fn handle_client_message(
    state: &AppState,
    room: &str,
    user: &str,
    text: &str,
    tx: &tokio::sync::mpsc::Sender<String>,
) {
    let msg: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    let msg_type = match msg.get("type").and_then(|t| t.as_str()) {
        Some(t) => t,
        None => return,
    };

    match msg_type {
        "claim" => {
            let Some(file_id) = msg.get("file_id").and_then(|v| v.as_i64()) else {
                return;
            };
            match state.claim_file(room, file_id, user) {
                Ok(true) => {
                    state.broadcast_to(
                        room,
                        &json!({ "type": "presence", "user": user, "file_id": file_id })
                            .to_string(),
                    );
                }
                Ok(false) => {
                    let owner = state.claim_owner(file_id);
                    let _ = tx
                        .send(
                            json!({ "type": "claim_rejected", "file_id": file_id, "owner": owner })
                                .to_string(),
                        )
                        .await;
                }
                Err(_) => {}
            }
        }
        "release" => {
            let Some(file_id) = msg.get("file_id").and_then(|v| v.as_i64()) else {
                return;
            };
            if state.release_file(file_id, user) {
                state.broadcast_to(
                    room,
                    &json!({ "type": "release", "user": user, "file_id": file_id }).to_string(),
                );
            }
        }
        "request_file" => {
            let Some(file_id) = msg.get("file_id").and_then(|v| v.as_i64()) else {
                return;
            };
            handle_file_request(state, room, user, file_id, tx).await;
        }
        "annotate" => {
            let Some(file_id) = msg.get("file_id").and_then(|v| v.as_i64()) else {
                return;
            };
            let Some(data) = msg.get("data") else { return };
            let is_check = data.get("is_check").and_then(|v| v.as_i64()).unwrap_or(0);
            let lab = data
                .get("lab")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let lab_without_tone = data
                .get("lab_without_tone")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let raw_text = data
                .get("raw_text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let annotation = crate::db::Annotation {
                file_id,
                is_check,
                lab,
                lab_without_tone,
                raw_text,
                annotated_by: Some(user.to_string()),
                updated_at: None,
            };
            if state.db.upsert_annotation(&annotation, user).is_err() {
                return;
            }
            let _ = state.db.set_file_status(file_id, "done", Some(user));
            state.release_file(file_id, user);

            let (done, total) = state.db.progress_for_room(room).unwrap_or((0, 0));
            state.broadcast_to(
                room,
                &json!({
                    "type": "annotated",
                    "user": user,
                    "file_id": file_id,
                    "data": {
                        "is_check": annotation.is_check,
                        "lab": annotation.lab,
                        "lab_without_tone": annotation.lab_without_tone,
                        "raw_text": annotation.raw_text,
                    },
                })
                .to_string(),
            );
            state.broadcast_to(
                room,
                &json!({ "type": "progress", "done": done, "total": total }).to_string(),
            );
        }
        _ => {}
    }
}

/// A room member asks for a file's audio. If the bytes are already on the
/// server the requester can download right away; otherwise the owning client
/// is told to upload them on demand.
async fn handle_file_request(
    state: &AppState,
    room: &str,
    user: &str,
    file_id: i64,
    tx: &tokio::sync::mpsc::Sender<String>,
) {
    let Ok(Some(file)) = state.db.get_file(file_id) else {
        return;
    };
    if file.room_id != room {
        return;
    }
    if file.uploaded {
        tracing::info!("room {room}: {user} requested file {file_id} (already uploaded)");
        let _ = tx
            .send(json!({ "type": "file_ready", "file_id": file_id }).to_string())
            .await;
        return;
    }
    let Some(owner) = file.owner else {
        tracing::info!("room {room}: {user} requested file {file_id} (no owner)");
        let _ = tx
            .send(json!({ "type": "file_unavailable", "file_id": file_id }).to_string())
            .await;
        return;
    };
    if owner == user {
        // The requester owns this file; nothing to relay.
        tracing::info!("room {room}: {user} requested own file {file_id}");
        return;
    }
    let sender = state
        .clients
        .lock()
        .unwrap()
        .get(&(room.to_string(), owner))
        .cloned();
    match sender {
        Some(s) => {
            tracing::info!("room {room}: relaying file {file_id} request to owner {owner}");
            let _ = s
                .send(json!({ "type": "file_requested", "file_id": file_id }).to_string())
                .await;
        }
        None => {
            tracing::info!("room {room}: owner {owner} of file {file_id} is offline");
            let _ = tx
                .send(json!({ "type": "file_unavailable", "file_id": file_id }).to_string())
                .await;
        }
    }
}
