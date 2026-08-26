// Package store persists roots, scans, files, and duplicate groups to SQLite.
package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS roots (
    id          INTEGER PRIMARY KEY,
    path        TEXT UNIQUE NOT NULL,
    label       TEXT NOT NULL DEFAULT '',
    added_at    DATETIME NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT 1,
    source      TEXT NOT NULL DEFAULT 'manual' -- 'manual' or 'auto' (discovered under /scan)
);

CREATE TABLE IF NOT EXISTS scans (
    id              INTEGER PRIMARY KEY,
    started_at      DATETIME NOT NULL,
    finished_at     DATETIME,
    status          TEXT NOT NULL,
    phase           TEXT NOT NULL DEFAULT '',
    files_seen      INTEGER NOT NULL DEFAULT 0,
    files_new       INTEGER NOT NULL DEFAULT 0,
    files_changed   INTEGER NOT NULL DEFAULT 0,
    files_removed   INTEGER NOT NULL DEFAULT 0,
    files_skipped   INTEGER NOT NULL DEFAULT 0,
    bytes_hashed    INTEGER NOT NULL DEFAULT 0,
    error           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS scan_roots (
    scan_id INTEGER NOT NULL REFERENCES scans(id),
    root_id INTEGER NOT NULL REFERENCES roots(id),
    PRIMARY KEY (scan_id, root_id)
);

CREATE TABLE IF NOT EXISTS files (
    id                  INTEGER PRIMARY KEY,
    root_id             INTEGER NOT NULL REFERENCES roots(id),
    rel_path            TEXT NOT NULL,
    size                INTEGER NOT NULL,
    mtime_ns            INTEGER NOT NULL,
    device              INTEGER NOT NULL,
    inode               INTEGER NOT NULL,
    partial_hash        TEXT NOT NULL DEFAULT '',
    full_hash           TEXT NOT NULL DEFAULT '',
    last_seen_scan_id   INTEGER,
    status              TEXT NOT NULL DEFAULT 'present',
    updated_at          DATETIME NOT NULL,
    UNIQUE (root_id, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_files_size         ON files(size);
CREATE INDEX IF NOT EXISTS idx_files_size_partial ON files(size, partial_hash);
CREATE INDEX IF NOT EXISTS idx_files_size_full    ON files(size, full_hash);
CREATE INDEX IF NOT EXISTS idx_files_dev_inode    ON files(device, inode);
CREATE INDEX IF NOT EXISTS idx_files_root         ON files(root_id);

CREATE TABLE IF NOT EXISTS duplicate_groups (
    id                      INTEGER PRIMARY KEY,
    size                    INTEGER NOT NULL,
    full_hash               TEXT NOT NULL,
    member_count            INTEGER NOT NULL,
    distinct_instance_count INTEGER NOT NULL,
    reclaimable_bytes       INTEGER NOT NULL,
    UNIQUE (size, full_hash)
);
CREATE INDEX IF NOT EXISTS idx_dupgroups_reclaimable ON duplicate_groups(reclaimable_bytes DESC);

CREATE TABLE IF NOT EXISTS duplicate_group_members (
    group_id INTEGER NOT NULL REFERENCES duplicate_groups(id),
    file_id  INTEGER NOT NULL REFERENCES files(id),
    PRIMARY KEY (group_id, file_id)
);
`

// Open opens (creating if necessary) the SQLite database at path and applies
// the schema. WAL mode is enabled so the web UI can keep reading duplicate
// data while a scan is writing.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	// SQLite handles one writer at a time; a single connection avoids
	// "database is locked" errors under concurrent goroutines within this
	// process and is fine for this app's throughput.
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma %q: %w", p, err)
		}
	}

	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &Store{db: db}, nil
}

// migrate applies schema changes that CREATE TABLE IF NOT EXISTS can't: an
// existing database from before a column was added needs it patched in.
func migrate(ctx context.Context, db *sql.DB) error {
	hasColumn, err := columnExists(ctx, db, "roots", "source")
	if err != nil {
		return err
	}
	if !hasColumn {
		if _, err := db.ExecContext(ctx,
			`ALTER TABLE roots ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'`); err != nil {
			return fmt.Errorf("add roots.source: %w", err)
		}
	}
	return nil
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) Close() error {
	return s.db.Close()
}

// DB exposes the underlying handle for packages that need bespoke queries
// (kept unexported-by-convention: only store subfiles should use this).
func (s *Store) conn() *sql.DB { return s.db }
