package store

import (
	"context"
	"fmt"
	"time"

	"github.com/mophead64/deduper/internal/model"
)

// StartScan records a new running scan covering the given roots.
func (s *Store) StartScan(ctx context.Context, rootIDs []int64) (int64, error) {
	res, err := s.conn().ExecContext(ctx,
		`INSERT INTO scans (started_at, status, phase) VALUES (?, ?, ?)`,
		time.Now().UTC(), model.ScanRunning, model.PhaseWalking)
	if err != nil {
		return 0, fmt.Errorf("start scan: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, rid := range rootIDs {
		if _, err := s.conn().ExecContext(ctx,
			`INSERT INTO scan_roots (scan_id, root_id) VALUES (?, ?)`, id, rid); err != nil {
			return 0, fmt.Errorf("link scan root: %w", err)
		}
	}
	return id, nil
}

// UpdateScanCounters overwrites the live progress counters for a running scan.
func (s *Store) UpdateScanCounters(ctx context.Context, scanID int64, phase model.ScanPhase, seen, added, changed, removed, skipped, bytesHashed int64) error {
	_, err := s.conn().ExecContext(ctx,
		`UPDATE scans SET phase = ?, files_seen = ?, files_new = ?, files_changed = ?,
		 files_removed = ?, files_skipped = ?, bytes_hashed = ? WHERE id = ?`,
		string(phase), seen, added, changed, removed, skipped, bytesHashed, scanID)
	return err
}

func (s *Store) FinishScan(ctx context.Context, scanID int64, status model.ScanStatus, errMsg string) error {
	_, err := s.conn().ExecContext(ctx,
		`UPDATE scans SET status = ?, finished_at = ?, error = ? WHERE id = ?`,
		string(status), time.Now().UTC(), errMsg, scanID)
	return err
}

// MarkInterruptedScans is called at startup: any scan left "running" from a
// previous process (e.g. the container was stopped mid-scan) could not have
// finished cleanly, so it's relabeled rather than left looking active forever.
func (s *Store) MarkInterruptedScans(ctx context.Context) error {
	_, err := s.conn().ExecContext(ctx,
		`UPDATE scans SET status = ?, finished_at = ? WHERE status = ?`,
		string(model.ScanInterrupted), time.Now().UTC(), string(model.ScanRunning))
	return err
}

func (s *Store) GetScan(ctx context.Context, id int64) (model.Scan, error) {
	var sc model.Scan
	var finishedAt *time.Time
	err := s.conn().QueryRowContext(ctx,
		`SELECT id, started_at, finished_at, status, phase, files_seen, files_new,
		 files_changed, files_removed, files_skipped, bytes_hashed, error
		 FROM scans WHERE id = ?`, id,
	).Scan(&sc.ID, &sc.StartedAt, &finishedAt, &sc.Status, &sc.Phase, &sc.FilesSeen,
		&sc.FilesNew, &sc.FilesChanged, &sc.FilesRemoved, &sc.FilesSkipped, &sc.BytesHashed, &sc.Error)
	if err != nil {
		return sc, err
	}
	sc.FinishedAt = finishedAt
	return sc, nil
}

func (s *Store) CurrentScan(ctx context.Context) (*model.Scan, error) {
	// The id lookup and GetScan below must not overlap: with a single-writer
	// SQLite connection, a Rows left open while a second query is issued on
	// the same *sql.DB blocks forever waiting for a connection that can't be
	// returned to the pool until this Rows is closed. Draining/closing rows
	// (via the explicit Close, not just defer) before calling GetScan avoids
	// that self-deadlock.
	rows, err := s.conn().QueryContext(ctx,
		`SELECT id FROM scans WHERE status = ? ORDER BY started_at DESC LIMIT 1`, string(model.ScanRunning))
	if err != nil {
		return nil, err
	}
	var id int64
	found := rows.Next()
	if found {
		err = rows.Scan(&id)
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	sc, err := s.GetScan(ctx, id)
	if err != nil {
		return nil, err
	}
	return &sc, nil
}

func (s *Store) ListScans(ctx context.Context, limit int) ([]model.Scan, error) {
	rows, err := s.conn().QueryContext(ctx,
		`SELECT id, started_at, finished_at, status, phase, files_seen, files_new,
		 files_changed, files_removed, files_skipped, bytes_hashed, error
		 FROM scans ORDER BY started_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Scan
	for rows.Next() {
		var sc model.Scan
		var finishedAt *time.Time
		if err := rows.Scan(&sc.ID, &sc.StartedAt, &finishedAt, &sc.Status, &sc.Phase, &sc.FilesSeen,
			&sc.FilesNew, &sc.FilesChanged, &sc.FilesRemoved, &sc.FilesSkipped, &sc.BytesHashed, &sc.Error); err != nil {
			return nil, err
		}
		sc.FinishedAt = finishedAt
		out = append(out, sc)
	}
	return out, rows.Err()
}
