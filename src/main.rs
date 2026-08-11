pub mod db;
pub mod http;
pub mod state;
pub mod ws;

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::Router;
use state::AppState;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let data_dir = std::env::var("MINLABEL_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"));
    std::fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let db_path = data_dir.join("minlabel.db");
    let audio_dir = data_dir.join("audio");
    std::fs::create_dir_all(&audio_dir).expect("failed to create audio dir");

    let state = AppState::new(&db_path, &audio_dir).expect("failed to init app state");

    let app = Router::new()
        .merge(http::router())
        .merge(ws::router())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = std::env::var("MINLABEL_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .expect("invalid MINLABEL_ADDR");

    tracing::info!("minlabel server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}
