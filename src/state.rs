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
    pub claims: Arc<Mutex<std::collections::HashMap<i64, String>>>,
    pub clients: Arc<Mutex<std::collections::HashMap<String, WsSender>>>,
    pub tx: broadcast::Sender<String>,
}

impl AppState {
    pub fn new(db_path: &std::path::Path, audio_dir: &PathBuf) -> Result<Self, rusqlite::Error> {
        let db = Arc::new(Db::open(db_path)?);
        let (tx, _) = broadcast::channel(256);
        Ok(Self {
            db,
            audio_dir: audio_dir.clone(),
            claims: Arc::new(Mutex::new(std::collections::HashMap::new())),
            clients: Arc::new(Mutex::new(std::collections::HashMap::new())),
            tx,
        })
    }

    pub fn claim_file(&self, file_id: i64, user: &str) -> Result<bool, String> {
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

    pub fn broadcast(&self, msg: &str) {
        let _ = self.tx.send(msg.to_string());
    }
}
