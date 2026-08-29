package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// idChunk bounds how many ids go into a single `IN (...)` statement. Well under
// SQLite's parameter limit, and small enough that one statement stays cheap.
const idChunk = 500

// ExistingFile is the lightweight per-file record the scanner preloads for a
// root at the start of a walk (one streaming query instead of a SELECT per
// file). The walk classifies each entry against this map in memory.
type ExistingFile struct {
	ID      int64
	Size    int64
	MTimeNs int64
	Device  uint64
	Inode   uint64
	Missing bool // was marked 'missing' by a previous scan
}

// LoadRootFiles returns every file ever recorded under rootID, keyed by
// rel_path. Missing files are included so a reappeared file is recognised
// rather than re-inserted.
func (s *Store) LoadRootFiles(ctx context.Context, rootID int64) (map[string]ExistingFile, error) {
	rows, err := s.reader().QueryContext(ctx,
		`SELECT rel_path, id, size, mtime_ns, device, inode, status FROM files WHERE root_id = ?`, rootID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]ExistingFile)
	for rows.Next() {
		var relPath, status string
		var f ExistingFile
		if err := rows.Scan(&relPath, &f.ID, &f.Size, &f.MTimeNs, &f.Device, &f.Inode, &status); err != nil {
			return nil, err
		}
		f.Missing = status == "missing"
		out[relPath] = f
	}
	return out, rows.Err()
}

// NewFile is one row to insert during the walk.
type NewFile struct {
	RelPath string
	Size    int64
	MTimeNs int64
	Device  uint64
	Inode   uint64
}

// ChangedFile is one existing row whose stat data moved since last seen; its
// stored hashes are cleared so the hash phase recomputes them.
type ChangedFile struct {
	ID      int64
	Size    int64
	MTimeNs int64
	Device  uint64
	Inode   uint64
}

// InsertFilesBatch inserts (or, on the rare race with an external creation,
// refreshes) a batch of newly-seen files inside one transaction.
func (s *Store) InsertFilesBatch(ctx context.Context, rootID, scanID int64, files []NewFile) error {
	if len(files) == 0 {
		return nil
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO files
			   (root_id, rel_path, size, mtime_ns, device, inode, partial_hash, full_hash, last_seen_scan_id, status, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, '', '', ?, 'present', ?)
			 ON CONFLICT(root_id, rel_path) DO UPDATE SET
			   size = excluded.size, mtime_ns = excluded.mtime_ns,
			   device = excluded.device, inode = excluded.inode,
			   last_seen_scan_id = excluded.last_seen_scan_id,
			   status = 'present', updated_at = excluded.updated_at`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		now := time.Now().UTC()
		for _, f := range files {
			if _, err := stmt.ExecContext(ctx, rootID, f.RelPath, f.Size, f.MTimeNs, f.Device, f.Inode, scanID, now); err != nil {
				return fmt.Errorf("insert file %s: %w", f.RelPath, err)
			}
		}
		return nil
	})
}

// UpdateFilesChangedBatch rewrites stat fields and clears stale hashes for a
// batch of changed files inside one transaction.
func (s *Store) UpdateFilesChangedBatch(ctx context.Context, scanID int64, files []ChangedFile) error {
	if len(files) == 0 {
		return nil
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`UPDATE files SET size = ?, mtime_ns = ?, device = ?, inode = ?,
			   partial_hash = '', full_hash = '', last_seen_scan_id = ?, status = 'present', updated_at = ?
			 WHERE id = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		now := time.Now().UTC()
		for _, f := range files {
			if _, err := stmt.ExecContext(ctx, f.Size, f.MTimeNs, f.Device, f.Inode, scanID, now, f.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// MarkFilesPresentBatch clears the 'missing' flag on files that reappeared
// unchanged, keeping their stored hashes. Usually a no-op (empty batch).
func (s *Store) MarkFilesPresentBatch(ctx context.Context, scanID int64, ids []int64) error {
	return s.updateByIDs(ctx, ids,
		`UPDATE files SET status = 'present', last_seen_scan_id = ?, updated_at = ? WHERE id IN (%s)`,
		scanID, time.Now().UTC())
}

// MarkFilesMissingBatch flags files not observed during the current scan.
func (s *Store) MarkFilesMissingBatch(ctx context.Context, ids []int64) error {
	return s.updateByIDs(ctx, ids,
		`UPDATE files SET status = 'missing', updated_at = ? WHERE id IN (%s)`,
		time.Now().UTC())
}

// updateByIDs runs query (a fmt template with one %s for the id placeholders)
// over ids in chunks, all inside one transaction. leadingArgs are bound before
// the id list.
func (s *Store) updateByIDs(ctx context.Context, ids []int64, query string, leadingArgs ...any) error {
	if len(ids) == 0 {
		return nil
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		for start := 0; start < len(ids); start += idChunk {
			end := start + idChunk
			if end > len(ids) {
				end = len(ids)
			}
			chunk := ids[start:end]
			placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
			args := make([]any, 0, len(leadingArgs)+len(chunk))
			args = append(args, leadingArgs...)
			for _, id := range chunk {
				args = append(args, id)
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(query, placeholders), args...); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

type HashCandidate struct {
	ID      int64
	RootID  int64
	RelPath string
	Size    int64
}

// HashResult is one computed hash to persist.
type HashResult struct {
	ID   int64
	Hash string
}

// PartialHashCandidatesPage returns up to limit present, unhashed files with
// id > afterID whose size collides with another present file — the next page of
// Stage 2 candidates. Paging (rather than one big slice or a long-lived cursor)
// keeps memory flat and lets WAL checkpoints run between pages.
func (s *Store) PartialHashCandidatesPage(ctx context.Context, afterID int64, limit int) ([]HashCandidate, error) {
	rows, err := s.reader().QueryContext(ctx,
		`SELECT f.id, f.root_id, f.rel_path, f.size FROM files f
		 WHERE f.status = 'present' AND f.full_hash = '' AND f.size > 0 AND f.id > ?
		 AND EXISTS (
		   SELECT 1 FROM files f2 WHERE f2.status = 'present' AND f2.size = f.size AND f2.id != f.id
		 )
		 ORDER BY f.id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCandidates(rows)
}

// FullHashCandidatesPage returns up to limit present files with id > afterID
// that survived Stage 2 — their (size, partial_hash) collides with another
// present file — and still need a full hash.
func (s *Store) FullHashCandidatesPage(ctx context.Context, afterID int64, limit int) ([]HashCandidate, error) {
	rows, err := s.reader().QueryContext(ctx,
		`SELECT f.id, f.root_id, f.rel_path, f.size FROM files f
		 WHERE f.status = 'present' AND f.full_hash = '' AND f.partial_hash != '' AND f.size > 0 AND f.id > ?
		 AND EXISTS (
		   SELECT 1 FROM files f2 WHERE f2.status = 'present' AND f2.size = f.size
		   AND f2.partial_hash = f.partial_hash AND f2.id != f.id
		 )
		 ORDER BY f.id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCandidates(rows)
}

func scanCandidates(rows *sql.Rows) ([]HashCandidate, error) {
	var out []HashCandidate
	for rows.Next() {
		var c HashCandidate
		if err := rows.Scan(&c.ID, &c.RootID, &c.RelPath, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpdatePartialHashesBatch persists a batch of Stage 2 hashes in one transaction.
func (s *Store) UpdatePartialHashesBatch(ctx context.Context, results []HashResult) error {
	return s.updateHashes(ctx, `UPDATE files SET partial_hash = ? WHERE id = ?`, results)
}

// UpdateFullHashesBatch persists a batch of Stage 4 hashes in one transaction.
func (s *Store) UpdateFullHashesBatch(ctx context.Context, results []HashResult) error {
	return s.updateHashes(ctx, `UPDATE files SET full_hash = ? WHERE id = ?`, results)
}

func (s *Store) updateHashes(ctx context.Context, query string, results []HashResult) error {
	if len(results) == 0 {
		return nil
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range results {
			if _, err := stmt.ExecContext(ctx, r.Hash, r.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// RootPath resolves a root's container path, used by the scanner to build
// absolute paths from the stored root-relative ones.
func (s *Store) RootPath(ctx context.Context, rootID int64) (string, error) {
	var p string
	err := s.reader().QueryRowContext(ctx, `SELECT path FROM roots WHERE id = ?`, rootID).Scan(&p)
	return p, err
}

// AllRootPaths returns every root's id→path, so the hash workers resolve paths
// from a map instead of a query per distinct root.
func (s *Store) AllRootPaths(ctx context.Context) (map[int64]string, error) {
	rows, err := s.reader().QueryContext(ctx, `SELECT id, path FROM roots`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]string)
	for rows.Next() {
		var id int64
		var path string
		if err := rows.Scan(&id, &path); err != nil {
			return nil, err
		}
		out[id] = path
	}
	return out, rows.Err()
}
