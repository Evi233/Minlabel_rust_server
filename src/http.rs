use axum::{
    extract::{Multipart, Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::db::Annotation;
use crate::state::AppState;

const ROOM_CODE_CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/rooms", post(create_room))
        .route(
            "/api/rooms/:room/files",
            get(list_room_files).post(register_files),
        )
        .route("/api/rooms/:room/files/:id/audio", post(upload_room_audio))
        .route("/api/files/:id/audio", get(download_audio))
        .route(
            "/api/annotations/:id",
            get(get_annotation).put(save_annotation),
        )
        .route("/api/progress", get(get_progress))
}

fn gen_room_code() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..6)
        .map(|_| {
            let i = rng.gen_range(0..ROOM_CODE_CHARS.len());
            ROOM_CODE_CHARS[i] as char
        })
        .collect()
}

#[derive(Deserialize)]
struct CreateRoomReq {
    user: String,
}

async fn create_room(
    State(state): State<AppState>,
    Json(req): Json<CreateRoomReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let user = req.user.trim();
    if user.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut code = gen_room_code();
    for _ in 0..10 {
        if state
            .db
            .room_exists(&code)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            code = gen_room_code();
        } else {
            break;
        }
    }
    state
        .db
        .create_room(&code, user)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "id": code })))
}

#[derive(Deserialize)]
struct RegisterFilesReq {
    user: String,
    files: Vec<RegisteredFile>,
}

#[derive(Deserialize)]
struct RegisteredFile {
    name: String,
    size: i64,
}

/// Register file metadata (name/size) without uploading the audio bytes.
/// The owner uploads each file later, on demand, when a room member asks for it.
async fn register_files(
    State(state): State<AppState>,
    Path(room): Path<String>,
    Json(req): Json<RegisterFilesReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if !state
        .db
        .room_exists(&room)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        return Err(StatusCode::NOT_FOUND);
    }
    let user = req.user.trim();
    if user.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut ids = Vec::new();
    for f in &req.files {
        let name = f.name.trim();
        if name.is_empty() {
            continue;
        }
        let id = state
            .db
            .insert_file(&room, user, name, f.size, "", None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        ids.push(json!({ "id": id, "name": name, "size": f.size }));
    }
    let (done, total) = state
        .db
        .progress_for_room(&room)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.broadcast_to(
        &room,
        &json!({ "type": "progress", "done": done, "total": total }).to_string(),
    );
    Ok(Json(json!({ "files": ids })))
}

async fn list_room_files(
    State(state): State<AppState>,
    Path(room): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let files = state
        .db
        .list_files_in_room(&room)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let files: Vec<serde_json::Value> = files
        .into_iter()
        .map(|f| {
            let owner = state.claim_owner(f.id);
            json!({
                "id": f.id,
                "name": f.name,
                "size": f.size,
                "uploaded": f.uploaded,
                "owner": f.owner,
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

/// The owning client uploads a file's audio bytes after another room member
/// requested it. Returns 409 if the file was already uploaded.
async fn upload_room_audio(
    State(state): State<AppState>,
    Path((room, id)): Path<(String, i64)>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut user: Option<String> = None;
    let mut data: Option<Vec<u8>> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        match field.name() {
            Some("user") => {
                user = Some(field.text().await.map_err(|_| StatusCode::BAD_REQUEST)?);
            }
            Some("file") => {
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
    let user = user.ok_or(StatusCode::BAD_REQUEST)?;
    let data = data.ok_or(StatusCode::BAD_REQUEST)?;

    let file = state
        .db
        .get_file(id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if file.room_id != room {
        return Err(StatusCode::NOT_FOUND);
    }
    if file.owner.as_deref() != Some(user.as_str()) {
        return Err(StatusCode::FORBIDDEN);
    }
    if file.uploaded {
        return Err(StatusCode::CONFLICT);
    }

    let uuid = uuid::Uuid::new_v4().to_string();
    let ext = std::path::Path::new(&file.name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("bin");
    let stored = format!("{uuid}.{ext}");
    let full_path = state.audio_dir.join(&stored);
    tokio::fs::write(&full_path, &data)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    state
        .db
        .mark_uploaded(id, &stored)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    state.broadcast_to(
        &room,
        &json!({ "type": "file_uploaded", "file_id": id }).to_string(),
    );
    Ok(Json(json!({ "id": id })))
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
    if !file.uploaded {
        return Err(StatusCode::NOT_FOUND);
    }
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
    let file = state
        .db
        .get_file(id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
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
        .progress_for_room(&file.room_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.broadcast_to(
        &file.room_id,
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
    state.broadcast_to(
        &file.room_id,
        &json!({ "type": "progress", "done": done, "total": total }).to_string(),
    );

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
