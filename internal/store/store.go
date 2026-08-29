// Package store persists roots, scans, files, and duplicate groups to SQLite.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"

	_ "modernc.org/sqlite"
)

// Store wraps two connection pools over the same SQLite file:
//
//   - rw: a single-connection pool for every write. SQLite allows exactly one
//     writer, so funnelling writes through one connection (with BEGIN IMMEDIATE
//     via _txlock) avoids "database is locked" churn between goroutines.
//   - ro: a multi-connection pool for reads. WAL mode lets readers run
//     concurrently with the writer against a consistent snapshot, so the web UI
//     stays responsive while a scan hammers the writer.
type Store struct {
	rw *sql.DB
	ro *sql.DB
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

// readPoolSize caps the reader pool. A handful of connections is plenty for the
// UI's query load and keeps the memory each idle SQLite connection holds modest.
func readPoolSize() int {
	n := runtime.NumCPU()
	if n < 4 {
		n = 4
	}
	if n > 8 {
		n = 8
	}
	return n
}

func openPool(ctx context.Context, path string, params []string, maxOpen int) (*sql.DB, error) {
	dsn := path
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Open opens (creating if necessary) the SQLite database at path and applies
// the schema. WAL mode is enabled so the reader pool can keep serving the UI
// while a scan writes through the single-writer pool.
func Open(ctx context.Context, path string) (*Store, error) {
	// BEGIN IMMEDIATE (via _txlock) makes the writer take its lock up front
	// rather than failing on upgrade halfway through a batch transaction.
	rw, err := openPool(ctx, path, []string{
		"_busy_timeout=5000",
		"_journal_mode=WAL",
		"_synchronous=NORMAL",
		"_foreign_keys=1",
		"_txlock=immediate",
	}, 1)
	if err != nil {
		return nil, fmt.Errorf("open write pool: %w", err)
	}

	if _, err := rw.ExecContext(ctx, schema); err != nil {
		rw.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := migrate(ctx, rw); err != nil {
		rw.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	ro, err := openPool(ctx, path, []string{
		"_busy_timeout=5000",
		"_foreign_keys=1",
		"_query_only=1",
	}, readPoolSize())
	if err != nil {
		rw.Close()
		return nil, fmt.Errorf("open read pool: %w", err)
	}

	return &Store{rw: rw, ro: ro}, nil
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
	err := s.ro.Close()
	if rwErr := s.rw.Close(); err == nil {
		err = rwErr
	}
	return err
}

// conn is the single-writer pool: everything that mutates the database.
func (s *Store) conn() *sql.DB { return s.rw }

// reader is the concurrent read pool: SELECTs that must not queue behind a
// running scan's writes.
func (s *Store) reader() *sql.DB { return s.ro }
