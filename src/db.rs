use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

#[derive(Debug, Clone, serde::Serialize)]
pub struct FileRecord {
    pub id: i64,
    pub name: String,
    pub path: String,
    pub duration: Option<f64>,
    pub status: String,
    pub annotated_by: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Annotation {
    pub file_id: i64,
    pub is_check: i64,
    pub lab: String,
    pub lab_without_tone: String,
    pub raw_text: String,
    pub annotated_by: Option<String>,
    pub updated_at: Option<String>,
}

pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS files (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT NOT NULL,
                 path TEXT NOT NULL,
                 duration REAL,
                 status TEXT NOT NULL DEFAULT 'pending',
                 annotated_by TEXT,
                 updated_at TEXT
             );
             CREATE TABLE IF NOT EXISTS annotations (
                 file_id INTEGER PRIMARY KEY REFERENCES files(id),
                 is_check INTEGER NOT NULL DEFAULT 0,
                 lab TEXT NOT NULL DEFAULT '',
                 lab_without_tone TEXT NOT NULL DEFAULT '',
                 raw_text TEXT NOT NULL DEFAULT '',
                 annotated_by TEXT,
                 updated_at TEXT
             );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn insert_file(
        &self,
        name: &str,
        path: &str,
        duration: Option<f64>,
    ) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO files (name, path, duration) VALUES (?1, ?2, ?3)",
            params![name, path, duration],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn list_files(&self) -> Result<Vec<FileRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT f.id, f.name, f.path, f.duration, f.status, f.annotated_by, f.updated_at
             FROM files f ORDER BY f.id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(FileRecord {
                id: r.get(0)?,
                name: r.get(1)?,
                path: r.get(2)?,
                duration: r.get(3)?,
                status: r.get(4)?,
                annotated_by: r.get(5)?,
                updated_at: r.get(6)?,
            })
        })?;
        rows.collect()
    }

    pub fn get_file(&self, id: i64) -> Result<Option<FileRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT f.id, f.name, f.path, f.duration, f.status, f.annotated_by, f.updated_at
             FROM files f WHERE f.id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |r| {
            Ok(FileRecord {
                id: r.get(0)?,
                name: r.get(1)?,
                path: r.get(2)?,
                duration: r.get(3)?,
                status: r.get(4)?,
                annotated_by: r.get(5)?,
                updated_at: r.get(6)?,
            })
        })?;
        rows.next().transpose()
    }

    pub fn set_file_status(
        &self,
        id: i64,
        status: &str,
        annotated_by: Option<&str>,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET status = ?1, annotated_by = ?2, updated_at = datetime('now') WHERE id = ?3",
            params![status, annotated_by, id],
        )?;
        Ok(())
    }

    pub fn get_annotation(&self, file_id: i64) -> Result<Option<Annotation>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT file_id, is_check, lab, lab_without_tone, raw_text, annotated_by, updated_at
             FROM annotations WHERE file_id = ?1",
        )?;
        let mut rows = stmt.query_map(params![file_id], |r| {
            Ok(Annotation {
                file_id: r.get(0)?,
                is_check: r.get(1)?,
                lab: r.get(2)?,
                lab_without_tone: r.get(3)?,
                raw_text: r.get(4)?,
                annotated_by: r.get(5)?,
                updated_at: r.get(6)?,
            })
        })?;
        rows.next().transpose()
    }

    pub fn upsert_annotation(
        &self,
        a: &Annotation,
        user: &str,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO annotations (file_id, is_check, lab, lab_without_tone, raw_text, annotated_by, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))
             ON CONFLICT(file_id) DO UPDATE SET
                 is_check = excluded.is_check,
                 lab = excluded.lab,
                 lab_without_tone = excluded.lab_without_tone,
                 raw_text = excluded.raw_text,
                 annotated_by = excluded.annotated_by,
                 updated_at = datetime('now')",
            params![a.file_id, a.is_check, a.lab, a.lab_without_tone, a.raw_text, user],
        )?;
        Ok(())
    }

    pub fn progress(&self) -> Result<(i64, i64), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))?;
        let done: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE status = 'done'",
            [],
            |r| r.get(0),
        )?;
        Ok((done, total))
    }
}
