package store

import (
	"context"
	"fmt"
	"time"

	"github.com/mophead64/deduper/internal/model"
)

func (s *Store) AddRoot(ctx context.Context, path, label string, source model.RootSource) (int64, error) {
	res, err := s.conn().ExecContext(ctx,
		`INSERT INTO roots (path, label, added_at, enabled, source) VALUES (?, ?, ?, 1, ?)`,
		path, label, time.Now().UTC(), string(source))
	if err != nil {
		return 0, fmt.Errorf("add root: %w", err)
	}
	return res.LastInsertId()
}

// DeleteRoot removes a root and everything recorded under it: its files, any
// duplicate-group memberships those files held, and its scan history links.
// SQLite would otherwise reject the plain `DELETE FROM roots` with a foreign
// key violation, since files.root_id references it; deleting the dependents
// explicitly (in a transaction, so a failure partway through leaves nothing
// half-deleted) gives the same effect as an ON DELETE CASCADE. The
// duplicate_groups table is then rebuilt from what remains, since groups that
// only had members under this root no longer have any.
func (s *Store) DeleteRoot(ctx context.Context, id int64) error {
	tx, err := s.conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM duplicate_group_members WHERE file_id IN (SELECT id FROM files WHERE root_id = ?)`, id,
	); err != nil {
		return fmt.Errorf("delete group memberships: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE root_id = ?`, id); err != nil {
		return fmt.Errorf("delete files: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM scan_roots WHERE root_id = ?`, id); err != nil {
		return fmt.Errorf("delete scan history links: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM roots WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete root: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return s.RebuildDuplicateGroups(ctx)
}

func (s *Store) SetRootEnabled(ctx context.Context, id int64, enabled bool) error {
	_, err := s.conn().ExecContext(ctx, `UPDATE roots SET enabled = ? WHERE id = ?`, enabled, id)
	return err
}

func (s *Store) ListRoots(ctx context.Context) ([]model.Root, error) {
	rows, err := s.reader().QueryContext(ctx,
		`SELECT id, path, label, added_at, enabled, source FROM roots ORDER BY added_at`)
	if err != nil {
		return nil, fmt.Errorf("list roots: %w", err)
	}
	defer rows.Close()

	var out []model.Root
	for rows.Next() {
		var r model.Root
		if err := rows.Scan(&r.ID, &r.Path, &r.Label, &r.AddedAt, &r.Enabled, &r.Source); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetRoot(ctx context.Context, id int64) (model.Root, error) {
	var r model.Root
	err := s.reader().QueryRowContext(ctx,
		`SELECT id, path, label, added_at, enabled, source FROM roots WHERE id = ?`, id,
	).Scan(&r.ID, &r.Path, &r.Label, &r.AddedAt, &r.Enabled, &r.Source)
	return r, err
}
