use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;

use crate::db::Db;

pub type WsSender = Sender<String>;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
    pub audio_dir: PathBuf,
    pub claims: Arc<Mutex<HashMap<i64, String>>>,
    /// (room, user) -> websocket message sender
    pub clients: Arc<Mutex<HashMap<(String, String), WsSender>>>,
    /// One broadcast channel per room, so messages never leak across rooms.
    pub room_tx: Arc<Mutex<HashMap<String, broadcast::Sender<String>>>>,
}

impl AppState {
    pub fn new(
        db_path: &std::path::Path,
        audio_dir: &std::path::Path,
    ) -> Result<Self, rusqlite::Error> {
        let db = Arc::new(Db::open(db_path)?);
        Ok(Self {
            db,
            audio_dir: audio_dir.to_path_buf(),
            claims: Arc::new(Mutex::new(HashMap::new())),
            clients: Arc::new(Mutex::new(HashMap::new())),
            room_tx: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn room_channel(&self, room: &str) -> broadcast::Sender<String> {
        self.room_tx
            .lock()
            .unwrap()
            .entry(room.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .clone()
    }

    pub fn claim_file(&self, room: &str, file_id: i64, user: &str) -> Result<bool, String> {
        if !self
            .db
            .file_in_room(file_id, room)
            .map_err(|e| e.to_string())?
        {
            return Ok(false);
        }
        let mut claims = self
            .claims
            .lock()
            .map_err(|_| "lock poisoned".to_string())?;
        if let Some(owner) = claims.get(&file_id) {
            if owner != user {
                return Ok(false);
            }
        }
        claims.insert(file_id, user.to_string());
        Ok(true)
    }

    pub fn release_file(&self, file_id: i64, user: &str) -> bool {
        let mut claims = self.claims.lock().map_err(|_| false).unwrap();
        if claims.get(&file_id).map(|o| o == user).unwrap_or(false) {
            claims.remove(&file_id);
            true
        } else {
            false
        }
    }

    pub fn release_all(&self, user: &str) -> Vec<i64> {
        let mut claims = self.claims.lock().unwrap();
        let released: Vec<i64> = claims
            .iter()
            .filter(|(_, owner)| owner.as_str() == user)
            .map(|(id, _)| *id)
            .collect();
        for id in &released {
            claims.remove(id);
        }
        released
    }

    pub fn claim_owner(&self, file_id: i64) -> Option<String> {
        self.claims.lock().unwrap().get(&file_id).cloned()
    }

    pub fn broadcast_to(&self, room: &str, msg: &str) {
        let _ = self.room_channel(room).send(msg.to_string());
    }
}
