package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mophead64/deduper/internal/model"
)

// FindFile returns the existing record for (rootID, relPath), or nil if none exists.
func (s *Store) FindFile(ctx context.Context, rootID int64, relPath string) (*model.FileRecord, error) {
	var r model.FileRecord
	err := s.conn().QueryRowContext(ctx,
		`SELECT id, root_id, rel_path, size, mtime_ns, device, inode, partial_hash, full_hash, status
		 FROM files WHERE root_id = ? AND rel_path = ?`, rootID, relPath,
	).Scan(&r.ID, &r.RootID, &r.RelPath, &r.Size, &r.MTimeNs, &r.Device, &r.Inode,
		&r.PartialHash, &r.FullHash, &r.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// InsertFile adds a brand new file record, seen for the first time in scanID.
func (s *Store) InsertFile(ctx context.Context, rootID int64, relPath string, size, mtimeNs int64, device, inode uint64, scanID int64) (int64, error) {
	res, err := s.conn().ExecContext(ctx,
		`INSERT INTO files (root_id, rel_path, size, mtime_ns, device, inode, partial_hash, full_hash,
		 last_seen_scan_id, status, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, '', '', ?, 'present', ?)`,
		rootID, relPath, size, mtimeNs, device, inode, scanID, time.Now().UTC())
	if err != nil {
		return 0, fmt.Errorf("insert file: %w", err)
	}
	return res.LastInsertId()
}

// UpdateFileChanged overwrites stat fields for a file whose size/mtime/identity
// changed since last seen, clearing its stale hashes so it gets re-hashed.
func (s *Store) UpdateFileChanged(ctx context.Context, id int64, size, mtimeNs int64, device, inode uint64, scanID int64) error {
	_, err := s.conn().ExecContext(ctx,
		`UPDATE files SET size = ?, mtime_ns = ?, device = ?, inode = ?, partial_hash = '', full_hash = '',
		 last_seen_scan_id = ?, status = 'present', updated_at = ? WHERE id = ?`,
		size, mtimeNs, device, inode, scanID, time.Now().UTC(), id)
	return err
}

// TouchFileSeen marks an unchanged file as seen in scanID without touching its hashes.
func (s *Store) TouchFileSeen(ctx context.Context, id int64, scanID int64) error {
	_, err := s.conn().ExecContext(ctx,
		`UPDATE files SET last_seen_scan_id = ?, status = 'present', updated_at = ? WHERE id = ?`,
		scanID, time.Now().UTC(), id)
	return err
}

// MarkMissing flags files under the given roots that were not observed during
// scanID as missing (e.g. deleted since the last scan of that root).
func (s *Store) MarkMissing(ctx context.Context, rootIDs []int64, scanID int64) (int64, error) {
	var total int64
	for _, rid := range rootIDs {
		res, err := s.conn().ExecContext(ctx,
			`UPDATE files SET status = 'missing', updated_at = ?
			 WHERE root_id = ? AND status = 'present' AND (last_seen_scan_id IS NULL OR last_seen_scan_id != ?)`,
			time.Now().UTC(), rid, scanID)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

type HashCandidate struct {
	ID      int64
	RootID  int64
	RelPath string
	Size    int64
}

// FilesNeedingPartialHash returns present files with no full_hash yet whose
// size collides with at least one other present file — i.e. files that just
// became duplicate *candidates* and are worth sampling.
func (s *Store) FilesNeedingPartialHash(ctx context.Context) ([]HashCandidate, error) {
	rows, err := s.conn().QueryContext(ctx,
		`SELECT f.id, f.root_id, f.rel_path, f.size FROM files f
		 WHERE f.status = 'present' AND f.full_hash = '' AND f.size > 0
		 AND EXISTS (
		   SELECT 1 FROM files f2 WHERE f2.status = 'present' AND f2.size = f.size AND f2.id != f.id
		 )`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCandidates(rows)
}

// FilesNeedingFullHash returns present files with no full_hash yet whose
// (size, partial_hash) collides with at least one other present file — i.e.
// candidates that survived the cheap sampling filter and warrant a full read.
func (s *Store) FilesNeedingFullHash(ctx context.Context) ([]HashCandidate, error) {
	rows, err := s.conn().QueryContext(ctx,
		`SELECT f.id, f.root_id, f.rel_path, f.size FROM files f
		 WHERE f.status = 'present' AND f.full_hash = '' AND f.partial_hash != '' AND f.size > 0
		 AND EXISTS (
		   SELECT 1 FROM files f2 WHERE f2.status = 'present' AND f2.size = f.size
		   AND f2.partial_hash = f.partial_hash AND f2.id != f.id
		 )`)
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

func (s *Store) UpdateFilePartialHash(ctx context.Context, id int64, hash string) error {
	_, err := s.conn().ExecContext(ctx, `UPDATE files SET partial_hash = ? WHERE id = ?`, hash, id)
	return err
}

func (s *Store) UpdateFileFullHash(ctx context.Context, id int64, hash string) error {
	_, err := s.conn().ExecContext(ctx, `UPDATE files SET full_hash = ? WHERE id = ?`, hash, id)
	return err
}

// RootPath resolves a root's container path, used by the scanner to build
// absolute paths from the stored root-relative ones.
func (s *Store) RootPath(ctx context.Context, rootID int64) (string, error) {
	var p string
	err := s.conn().QueryRowContext(ctx, `SELECT path FROM roots WHERE id = ?`, rootID).Scan(&p)
	return p, err
}
