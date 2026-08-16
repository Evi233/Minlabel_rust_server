//! End-to-end test of the room flow with two clients:
//! create room -> register metadata -> peer requests a file ->
//! owner uploads on demand -> requester downloads -> annotate.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use minlabel_server::state::AppState;

fn test_state(name: &str) -> AppState {
    let dir = std::env::temp_dir().join(format!("minlabel-test-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("audio")).unwrap();
    AppState::new(&dir.join("test.db"), &dir.join("audio")).unwrap()
}

async fn spawn_server(state: AppState) -> SocketAddr {
    let app = Router::new()
        .merge(minlabel_server::http::router())
        .merge(minlabel_server::ws::router())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_ws(addr: SocketAddr, user: &str, room: &str) -> WsStream {
    let url = format!("ws://{addr}/ws?user={user}&room={room}");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws
}

/// Read WS text messages until one matches the predicate; return it.
async fn wait_for_msg<F>(ws: &mut WsStream, mut pred: F) -> Value
where
    F: FnMut(&Value) -> bool,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        if pred(&v) {
                            return v;
                        }
                    }
                }
                Some(Ok(WsMessage::Close(_))) => panic!("ws closed unexpectedly"),
                Some(Err(e)) => panic!("ws error: {e}"),
                None => panic!("ws stream ended"),
                _ => {}
            }
        }
    })
    .await
    .expect("timed out waiting for ws message")
}

fn post(url: &str, body: Value) -> Value {
    let resp = reqwest::blocking::Client::new()
        .post(url)
        .json(&body)
        .send()
        .unwrap();
    assert!(
        resp.status().is_success(),
        "POST {url} failed: {}",
        resp.status()
    );
    resp.json().unwrap()
}

#[tokio::test]
async fn full_room_flow() {
    let state = test_state("full");
    let addr = spawn_server(state).await;
    let http = format!("http://{addr}");
    let client = reqwest::blocking::Client::new();

    // --- Alice creates a room ---
    let room: Value = post(&format!("{http}/api/rooms"), json!({ "user": "alice" }));
    let code = room["id"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 6);

    // --- Alice registers file metadata only (no bytes) ---
    let reg: Value = post(
        &format!("{http}/api/rooms/{code}/files"),
        json!({ "user": "alice", "files": [
            { "name": "a.wav", "size": 4 },
            { "name": "b.wav", "size": 8 },
        ]}),
    );
    let files = reg["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    let a_id = files[0]["id"].as_i64().unwrap();
    let b_id = files[1]["id"].as_i64().unwrap();

    // --- A fresh file list shows them as not uploaded ---
    let list: Value = client
        .get(format!("{http}/api/rooms/{code}/files"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(list["files"][0]["uploaded"], false);
    assert_eq!(list["files"][0]["owner"], "alice");
    assert_eq!(list["files"][0]["size"], 4);

    // --- Both clients join over websocket ---
    let mut alice_ws = connect_ws(addr, "alice", &code).await;
    let mut bob_ws = connect_ws(addr, "bob", &code).await;

    // --- Bob asks for a.wav; Alice is told to upload it ---
    bob_ws
        .send(WsMessage::Text(
            json!({ "type": "request_file", "file_id": a_id }).to_string(),
        ))
        .await
        .unwrap();
    let req = wait_for_msg(&mut alice_ws, |m| m["type"] == "file_requested").await;
    assert_eq!(req["file_id"], a_id);

    // --- Alice uploads the bytes on demand ---
    let audio: Vec<u8> = b"RIFF".to_vec();
    let form = reqwest::blocking::multipart::Form::new()
        .text("user", "alice")
        .part(
            "file",
            reqwest::blocking::multipart::Part::bytes(audio.clone()).file_name("a.wav"),
        );
    let up: Value = client
        .post(format!("{http}/api/rooms/{code}/files/{a_id}/audio"))
        .multipart(form)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(up["id"], a_id);

    // --- Bob hears about the upload and downloads ---
    let evt = wait_for_msg(&mut bob_ws, |m| m["type"] == "file_uploaded").await;
    assert_eq!(evt["file_id"], a_id);
    let down = client
        .get(format!("{http}/api/files/{a_id}/audio"))
        .send()
        .unwrap();
    assert_eq!(down.bytes().unwrap().as_ref(), audio.as_slice());

    // --- Re-requesting the same file is answered directly (file_ready) ---
    bob_ws
        .send(WsMessage::Text(
            json!({ "type": "request_file", "file_id": a_id }).to_string(),
        ))
        .await
        .unwrap();
    let ready = wait_for_msg(&mut bob_ws, |m| m["type"] == "file_ready").await;
    assert_eq!(ready["file_id"], a_id);

    // --- A non-owner cannot upload ---
    let form = reqwest::blocking::multipart::Form::new()
        .text("user", "bob")
        .part(
            "file",
            reqwest::blocking::multipart::Part::bytes(b"XXXX".to_vec()).file_name("b.wav"),
        );
    let resp = client
        .post(format!("{http}/api/rooms/{code}/files/{b_id}/audio"))
        .multipart(form)
        .send()
        .unwrap();
    assert_eq!(resp.status(), 403);

    // --- Bob annotates; Alice sees it over the room channel ---
    let resp = client
        .put(format!("{http}/api/annotations/{a_id}"))
        .json(&json!({
            "user": "bob",
            "is_check": 1, "lab": "ni hao", "lab_without_tone": "ni hao", "raw_text": "nihao"
        }))
        .send()
        .unwrap();
    assert_eq!(resp.status(), 200);
    let annotated = wait_for_msg(&mut alice_ws, |m| m["type"] == "annotated").await;
    assert_eq!(annotated["file_id"], a_id);
    assert_eq!(annotated["user"], "bob");
    assert_eq!(annotated["data"]["lab"], "ni hao");

    // --- Presence: Bob claims the file ---
    bob_ws
        .send(WsMessage::Text(
            json!({ "type": "claim", "file_id": b_id }).to_string(),
        ))
        .await
        .unwrap();
    let presence = wait_for_msg(&mut alice_ws, |m| m["type"] == "presence").await;
    assert_eq!(presence["user"], "bob");
    assert_eq!(presence["file_id"], b_id);
}

#[tokio::test]
async fn offline_owner_is_reported() {
    let state = test_state("offline");
    let addr = spawn_server(state).await;
    let http = format!("http://{addr}");

    let room: Value = post(&format!("{http}/api/rooms"), json!({ "user": "alice" }));
    let code = room["id"].as_str().unwrap().to_string();
    let reg: Value = post(
        &format!("{http}/api/rooms/{code}/files"),
        json!({ "user": "alice", "files": [{ "name": "a.wav", "size": 4 }] }),
    );
    let a_id = reg["files"][0]["id"].as_i64().unwrap();

    // Alice never connects; Bob's request should report the file as unavailable.
    let mut bob_ws = connect_ws(addr, "bob", &code).await;
    bob_ws
        .send(WsMessage::Text(
            json!({ "type": "request_file", "file_id": a_id }).to_string(),
        ))
        .await
        .unwrap();
    let evt = wait_for_msg(&mut bob_ws, |m| m["type"] == "file_unavailable").await;
    assert_eq!(evt["file_id"], a_id);

    // Unknown room is rejected at connect time.
    let err = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?user=x&room=NOPE")).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn rooms_are_isolated() {
    let state = test_state("iso");
    let addr = spawn_server(state).await;
    let http = format!("http://{addr}");
    let client = reqwest::blocking::Client::new();

    let room_a: Value = post(&format!("{http}/api/rooms"), json!({ "user": "a" }));
    let room_b: Value = post(&format!("{http}/api/rooms"), json!({ "user": "a" }));
    assert_ne!(room_a["id"], room_b["id"]);

    let reg_a: Value = post(
        &format!("{http}/api/rooms/{}/files", room_a["id"]),
        json!({ "user": "a", "files": [{ "name": "a.wav", "size": 1 }] }),
    );
    let a_id = reg_a["files"][0]["id"].as_i64().unwrap();

    // The other room's list must not contain it.
    let list_b: Value = client
        .get(format!("{http}/api/rooms/{}/files", room_b["id"]))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(list_b["files"].as_array().unwrap().len(), 0);

    // Claiming a file from outside its room is rejected.
    let mut ws_b = connect_ws(addr, "bob", room_b["id"].as_str().unwrap()).await;
    ws_b.send(WsMessage::Text(
        json!({ "type": "claim", "file_id": a_id }).to_string(),
    ))
    .await
    .unwrap();
    let rejected = wait_for_msg(&mut ws_b, |m| m["type"] == "claim_rejected").await;
    assert_eq!(rejected["file_id"], a_id);
}
