use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
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
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, q.user, state))
}

async fn handle_socket(socket: WebSocket, user: String, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(64);

    state
        .clients
        .lock()
        .unwrap()
        .insert(user.clone(), tx.clone());

    let mut broadcast_rx = state.tx.subscribe();
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(text) => {
                            if sender.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                msg = broadcast_rx.recv() => {
                    match msg {
                        Ok(text) => {
                            if sender.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    let (done, total) = state.db.progress().unwrap_or((0, 0));
    let _ = tx
        .send(json!({ "type": "progress", "done": done, "total": total }).to_string())
        .await;

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                handle_client_message(&state, &user, &text, &tx).await;
            }
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Binary(_) => {}
        }
    }

    let released = state.release_all(&user);
    for id in released {
        state.broadcast(&json!({ "type": "release", "user": user, "file_id": id }).to_string());
    }
    state.clients.lock().unwrap().remove(&user);
    send_task.abort();
}

async fn handle_client_message(
    state: &AppState,
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
            match state.claim_file(file_id, user) {
                Ok(true) => {
                    state.broadcast(
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
                state.broadcast(
                    &json!({ "type": "release", "user": user, "file_id": file_id }).to_string(),
                );
            }
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

            let (done, total) = state.db.progress().unwrap_or((0, 0));
            state.broadcast(
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
            state.broadcast(
                &json!({ "type": "progress", "done": done, "total": total }).to_string(),
            );
        }
        _ => {}
    }
}
