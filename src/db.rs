use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

#[derive(Debug, Clone, serde::Serialize)]
pub struct FileRecord {
    pub id: i64,
    pub room_id: String,
    pub name: String,
    pub path: String,
    pub size: i64,
    pub uploaded: bool,
    pub owner: Option<String>,
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
             CREATE TABLE IF NOT EXISTS rooms (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL DEFAULT '',
                 owner TEXT NOT NULL,
                 created_at TEXT
             );
             CREATE TABLE IF NOT EXISTS files (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 room_id TEXT NOT NULL DEFAULT '',
                 owner TEXT,
                 name TEXT NOT NULL,
                 path TEXT NOT NULL DEFAULT '',
                 size INTEGER NOT NULL DEFAULT 0,
                 uploaded INTEGER NOT NULL DEFAULT 0,
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
        Self::migrate_columns(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Add room-related columns to a `files` table created by an older version,
    /// and move any legacy rows (no room) into a `legacy` room.
    fn migrate_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
        let mut existing = Vec::new();
        {
            let mut stmt = conn.prepare("PRAGMA table_info(files)")?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                existing.push(r.get::<_, String>(1)?);
            }
        }
        let add_cols: &[(&str, &str)] = &[
            ("room_id", "TEXT NOT NULL DEFAULT ''"),
            ("owner", "TEXT"),
            ("path", "TEXT NOT NULL DEFAULT ''"),
            ("size", "INTEGER NOT NULL DEFAULT 0"),
            ("uploaded", "INTEGER NOT NULL DEFAULT 0"),
        ];
        for (col, ddl) in add_cols {
            if !existing.iter().any(|c| c == col) {
                conn.execute_batch(&format!("ALTER TABLE files ADD COLUMN {col} {ddl};"))?;
            }
        }
        // Rows that already have audio bytes on disk are uploaded.
        conn.execute_batch("UPDATE files SET uploaded = 1 WHERE path <> '' AND uploaded = 0;")?;
        // Put legacy rows into a `legacy` room so they stay reachable.
        let legacy: i64 =
            conn.query_row("SELECT COUNT(*) FROM files WHERE room_id = ''", [], |r| {
                r.get(0)
            })?;
        if legacy > 0 {
            conn.execute(
                "INSERT OR IGNORE INTO rooms (id, name, owner, created_at) VALUES ('legacy', '', '', datetime('now'))",
                [],
            )?;
            conn.execute("UPDATE files SET room_id = 'legacy' WHERE room_id = ''", [])?;
        }
        Ok(())
    }

    pub fn create_room(&self, id: &str, owner: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO rooms (id, name, owner, created_at) VALUES (?1, '', ?2, datetime('now'))",
            params![id, owner],
        )?;
        Ok(())
    }

    pub fn room_exists(&self, id: &str) -> Result<bool, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM rooms WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    pub fn insert_file(
        &self,
        room_id: &str,
        owner: &str,
        name: &str,
        size: i64,
        path: &str,
        duration: Option<f64>,
    ) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO files (room_id, owner, name, path, size, uploaded, duration)
             VALUES (?1, ?2, ?3, ?4, ?5, CASE WHEN ?4 = '' THEN 0 ELSE 1 END, ?6)",
            params![room_id, owner, name, path, size, duration],
        )?;
        Ok(conn.last_insert_rowid())
    }

    fn record_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
        Ok(FileRecord {
            id: r.get(0)?,
            room_id: r.get(1)?,
            name: r.get(2)?,
            path: r.get(3)?,
            size: r.get(4)?,
            uploaded: r.get::<_, i64>(5)? != 0,
            owner: r.get(6)?,
            duration: r.get(7)?,
            status: r.get(8)?,
            annotated_by: r.get(9)?,
            updated_at: r.get(10)?,
        })
    }

    const FILE_COLS: &'static str = "f.id, f.room_id, f.name, f.path, f.size, f.uploaded, \
                                     f.owner, f.duration, f.status, f.annotated_by, f.updated_at";

    pub fn list_files_in_room(&self, room_id: &str) -> Result<Vec<FileRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM files f WHERE f.room_id = ?1 ORDER BY f.id",
            Self::FILE_COLS
        ))?;
        let rows = stmt.query_map(params![room_id], Self::record_from_row)?;
        rows.collect()
    }

    pub fn get_file(&self, id: i64) -> Result<Option<FileRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM files f WHERE f.id = ?1",
            Self::FILE_COLS
        ))?;
        let mut rows = stmt.query_map(params![id], Self::record_from_row)?;
        rows.next().transpose()
    }

    pub fn file_in_room(&self, id: i64, room_id: &str) -> Result<bool, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE id = ?1 AND room_id = ?2",
            params![id, room_id],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Record that a file's audio bytes have been uploaded to the server.
    pub fn mark_uploaded(&self, id: i64, stored_path: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET uploaded = 1, path = ?2 WHERE id = ?1",
            params![id, stored_path],
        )?;
        Ok(())
    }

    pub fn progress_for_room(&self, room_id: &str) -> Result<(i64, i64), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE room_id = ?1",
            params![room_id],
            |r| r.get(0),
        )?;
        let done: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE room_id = ?1 AND status = 'done'",
            params![room_id],
            |r| r.get(0),
        )?;
        Ok((done, total))
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

    pub fn upsert_annotation(&self, a: &Annotation, user: &str) -> Result<(), rusqlite::Error> {
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
