use axum::{
    extract::{Multipart, Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::db::Annotation;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/files", get(list_files).post(upload_file))
        .route("/api/files/{id}/audio", get(download_audio))
        .route(
            "/api/annotations/{id}",
            get(get_annotation).put(save_annotation),
        )
        .route("/api/progress", get(get_progress))
}

async fn list_files(State(state): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let files = state
        .db
        .list_files()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let files: Vec<serde_json::Value> = files
        .into_iter()
        .map(|f| {
            let owner = state.claim_owner(f.id);
            json!({
                "id": f.id,
                "name": f.name,
                "duration": f.duration,
                "status": f.status,
                "annotated_by": f.annotated_by,
                "updated_at": f.updated_at,
                "annotating_by": owner,
            })
        })
        .collect();
    Ok(Json(json!({ "files": files })))
}

async fn upload_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut name: Option<String> = None;
    let mut data: Option<Vec<u8>> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        match field.name() {
            Some("file") => {
                name = field.file_name().map(|s| s.to_string());
                data = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|_| StatusCode::BAD_REQUEST)?
                        .to_vec(),
                );
            }
            _ => {}
        }
    }
    let name = name.ok_or(StatusCode::BAD_REQUEST)?;
    let data = data.ok_or(StatusCode::BAD_REQUEST)?;

    let id = uuid::Uuid::new_v4().to_string();
    let ext = std::path::Path::new(&name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("bin");
    let stored = format!("{id}.{ext}");
    let full_path = state.audio_dir.join(&stored);
    tokio::fs::write(&full_path, &data)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let file_id = state
        .db
        .insert_file(&name, &stored, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (done, total) = state
        .db
        .progress()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.broadcast(&json!({ "type": "progress", "done": done, "total": total }).to_string());

    Ok(Json(json!({ "id": file_id, "name": name })))
}

async fn download_audio(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Response, StatusCode> {
    let file = state
        .db
        .get_file(id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let file = file.ok_or(StatusCode::NOT_FOUND)?;
    let full_path = state.audio_dir.join(&file.path);
    let bytes = tokio::fs::read(&full_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let mime = mime_for(&file.name);
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", file.name))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(mime)),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        bytes,
    )
        .into_response())
}

async fn get_annotation(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state
        .db
        .get_annotation(id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(a) => Ok(Json(json!({
            "file_id": a.file_id,
            "is_check": a.is_check,
            "lab": a.lab,
            "lab_without_tone": a.lab_without_tone,
            "raw_text": a.raw_text,
            "annotated_by": a.annotated_by,
            "updated_at": a.updated_at,
        }))),
        None => Ok(Json(json!({
            "file_id": id,
            "is_check": 0,
            "lab": "",
            "lab_without_tone": "",
            "raw_text": "",
            "annotated_by": null,
            "updated_at": null,
        }))),
    }
}

#[derive(Deserialize)]
struct SaveAnnotationReq {
    is_check: i64,
    lab: String,
    lab_without_tone: String,
    raw_text: String,
    user: String,
}

async fn save_annotation(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<SaveAnnotationReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if state
        .db
        .get_file(id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_none()
    {
        return Err(StatusCode::NOT_FOUND);
    }
    let annotation = Annotation {
        file_id: id,
        is_check: req.is_check,
        lab: req.lab,
        lab_without_tone: req.lab_without_tone,
        raw_text: req.raw_text,
        annotated_by: Some(req.user.clone()),
        updated_at: None,
    };
    state
        .db
        .upsert_annotation(&annotation, &req.user)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .db
        .set_file_status(id, "done", Some(&req.user))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.release_file(id, &req.user);

    let (done, total) = state
        .db
        .progress()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.broadcast(
        &json!({
            "type": "annotated",
            "user": req.user,
            "file_id": id,
            "data": {
                "is_check": annotation.is_check,
                "lab": annotation.lab,
                "lab_without_tone": annotation.lab_without_tone,
                "raw_text": annotation.raw_text,
            },
        })
        .to_string(),
    );
    state.broadcast(&json!({ "type": "progress", "done": done, "total": total }).to_string());

    Ok(Json(json!({ "ok": true })))
}

async fn get_progress(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let (done, total) = state
        .db
        .progress()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "done": done, "total": total })))
}

fn mime_for(name: &str) -> &'static str {
    match std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
    {
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "ogg" => "audio/ogg",
        "m4a" | "aac" => "audio/mp4",
        _ => "application/octet-stream",
    }
}
