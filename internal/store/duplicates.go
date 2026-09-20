package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mophead64/deduper/internal/model"
)

// RebuildDuplicateGroups recomputes the materialized duplicate_groups and
// duplicate_group_members tables from the current files table. It runs after
// every scan (see Stage 5 in README.md): group present, hashed files by
// (size, full_hash); a group's distinct_instance_count counts distinct
// (device, inode) pairs so hardlinked copies of the same data collapse to one
// instance and don't inflate reclaimable space.
//
// The whole rebuild is three set-based statements — no per-group round trips and
// no loading every group key into a Go slice — so it stays cheap and flat in
// memory even with millions of files.
func (s *Store) RebuildDuplicateGroups(ctx context.Context) error {
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM duplicate_group_members`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM duplicate_groups`); err != nil {
			return err
		}
		// COUNT(DISTINCT device || '/' || inode) collapses hardlinks: several
		// paths sharing one (device, inode) count as a single on-disk instance.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO duplicate_groups (size, full_hash, member_count, distinct_instance_count, reclaimable_bytes)
			 SELECT f.size, f.full_hash, COUNT(*),
			        COUNT(DISTINCT f.device || '/' || f.inode),
			        (COUNT(DISTINCT f.device || '/' || f.inode) - 1) * f.size
			 FROM files f
			 WHERE f.status = 'present' AND f.full_hash != '' AND f.size > 0
			 GROUP BY f.size, f.full_hash
			 HAVING COUNT(*) > 1`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO duplicate_group_members (group_id, file_id)
			 SELECT g.id, f.id
			 FROM files f
			 JOIN duplicate_groups g ON g.size = f.size AND g.full_hash = f.full_hash
			 WHERE f.status = 'present' AND f.full_hash != '' AND f.size > 0`); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		s.summaryMu.Lock()
		s.summaryAt = time.Time{}
		s.summaryMu.Unlock()
	}
	return err
}

type DuplicateFilter struct {
	RootID   int64 // 0 = all roots
	MinSize  int64
	NameLike string // substring match against rel_path
	Page     int
	PageSize int
}

func (s *Store) ListDuplicateGroups(ctx context.Context, f DuplicateFilter) ([]model.DuplicateGroup, int, error) {
	if f.Page < 1 {
		f.Page = 1
	}
	if f.PageSize < 1 {
		f.PageSize = 25
	}

	where := `WHERE dg.size >= ?`
	args := []any{f.MinSize}
	if f.RootID != 0 || f.NameLike != "" {
		where += ` AND EXISTS (
			SELECT 1 FROM duplicate_group_members m JOIN files fl ON fl.id = m.file_id
			WHERE m.group_id = dg.id`
		if f.RootID != 0 {
			where += ` AND fl.root_id = ?`
			args = append(args, f.RootID)
		}
		if f.NameLike != "" {
			where += ` AND fl.rel_path LIKE ?`
			args = append(args, "%"+f.NameLike+"%")
		}
		where += `)`
	}

	var total int
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM duplicate_groups dg %s`, where)
	if err := s.reader().QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	query := fmt.Sprintf(
		`SELECT dg.id, dg.size, dg.full_hash, dg.member_count, dg.distinct_instance_count, dg.reclaimable_bytes
		 FROM duplicate_groups dg %s
		 ORDER BY dg.reclaimable_bytes DESC, dg.size DESC
		 LIMIT ? OFFSET ?`, where)
	args = append(args, f.PageSize, (f.Page-1)*f.PageSize)

	rows, err := s.reader().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var out []model.DuplicateGroup
	for rows.Next() {
		var g model.DuplicateGroup
		if err := rows.Scan(&g.ID, &g.Size, &g.FullHash, &g.MemberCount, &g.DistinctInstanceCount, &g.ReclaimableBytes); err != nil {
			return nil, 0, err
		}
		out = append(out, g)
	}
	return out, total, rows.Err()
}

func (s *Store) GetDuplicateGroup(ctx context.Context, id int64) (model.DuplicateGroup, error) {
	var g model.DuplicateGroup
	err := s.reader().QueryRowContext(ctx,
		`SELECT id, size, full_hash, member_count, distinct_instance_count, reclaimable_bytes
		 FROM duplicate_groups WHERE id = ?`, id,
	).Scan(&g.ID, &g.Size, &g.FullHash, &g.MemberCount, &g.DistinctInstanceCount, &g.ReclaimableBytes)
	return g, err
}

func (s *Store) GetDuplicateGroupMembers(ctx context.Context, groupID int64) ([]model.DuplicateGroupMember, error) {
	rows, err := s.reader().QueryContext(ctx,
		`SELECT fl.id, fl.root_id, r.path, fl.rel_path, fl.size, fl.mtime_ns, fl.device, fl.inode
		 FROM duplicate_group_members m
		 JOIN files fl ON fl.id = m.file_id
		 JOIN roots r ON r.id = fl.root_id
		 WHERE m.group_id = ?
		 ORDER BY r.path, fl.rel_path`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.DuplicateGroupMember
	for rows.Next() {
		var m model.DuplicateGroupMember
		if err := rows.Scan(&m.FileID, &m.RootID, &m.RootPath, &m.RelPath, &m.Size, &m.MTimeNs, &m.Device, &m.Inode); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

type Summary struct {
	TotalFiles       int64
	TotalBytes       int64
	TotalGroups      int64
	TotalReclaimable int64
}

// summaryTTL bounds how stale the dashboard totals can be. Aggregating every
// present file is O(files), so on multi-million-row databases it is computed at
// most once per TTL no matter how often the dashboard is loaded.
const summaryTTL = 30 * time.Second

func (s *Store) SummaryStats(ctx context.Context) (Summary, error) {
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()
	if !s.summaryAt.IsZero() && time.Since(s.summaryAt) < summaryTTL {
		return s.summary, nil
	}

	var sum Summary
	err := s.reader().QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE status = 'present'`,
	).Scan(&sum.TotalFiles, &sum.TotalBytes)
	if err != nil {
		return sum, err
	}
	err = s.reader().QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(reclaimable_bytes), 0) FROM duplicate_groups`,
	).Scan(&sum.TotalGroups, &sum.TotalReclaimable)
	if err != nil {
		return sum, err
	}
	s.summary, s.summaryAt = sum, time.Now()
	return sum, nil
}

// GroupRescanUpdate is one member whose on-disk stat data no longer matches the
// store. The scanner re-hashes it, so Hash is its current full hash.
type GroupRescanUpdate struct {
	ID      int64
	Size    int64
	MTimeNs int64
	Device  uint64
	Inode   uint64
	Hash    string
}

// ApplyGroupRescan folds the result of re-checking one duplicate group's files
// into the current state without touching scan history or any other group:
//   - missing files are flagged 'missing' and leave the group;
//   - updated files get their fresh stat data and hash, and stay in the group
//     only if they still match it (a changed file that no longer matches is
//     picked up by the next full scan's regroup);
//   - the group's counts are recomputed, and the group is deleted if fewer than
//     two members remain.
//
// It reports whether the group still exists.
func (s *Store) ApplyGroupRescan(ctx context.Context, groupID int64, missing []int64, updates []GroupRescanUpdate) (exists bool, err error) {
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		var size int64
		var hash string
		if err := tx.QueryRowContext(ctx,
			`SELECT size, full_hash FROM duplicate_groups WHERE id = ?`, groupID).Scan(&size, &hash); err != nil {
			return err
		}
		now := time.Now().UTC()

		for _, id := range missing {
			if _, err := tx.ExecContext(ctx,
				`UPDATE files SET status = 'missing', updated_at = ? WHERE id = ?`, now, id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM duplicate_group_members WHERE group_id = ? AND file_id = ?`, groupID, id); err != nil {
				return err
			}
		}
		for _, u := range updates {
			if _, err := tx.ExecContext(ctx,
				`UPDATE files SET size = ?, mtime_ns = ?, device = ?, inode = ?,
				   partial_hash = '', full_hash = ?, status = 'present', updated_at = ?
				 WHERE id = ?`, u.Size, u.MTimeNs, u.Device, u.Inode, u.Hash, now, u.ID); err != nil {
				return err
			}
			if u.Size != size || u.Hash != hash {
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM duplicate_group_members WHERE group_id = ? AND file_id = ?`, groupID, u.ID); err != nil {
					return err
				}
			}
		}

		var members, instances int64
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*), COUNT(DISTINCT f.device || '/' || f.inode)
			 FROM duplicate_group_members m JOIN files f ON f.id = m.file_id
			 WHERE m.group_id = ?`, groupID).Scan(&members, &instances); err != nil {
			return err
		}
		if members < 2 {
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM duplicate_group_members WHERE group_id = ?`, groupID); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `DELETE FROM duplicate_groups WHERE id = ?`, groupID)
			return err
		}
		exists = true
		_, err := tx.ExecContext(ctx,
			`UPDATE duplicate_groups SET member_count = ?, distinct_instance_count = ?, reclaimable_bytes = ? WHERE id = ?`,
			members, instances, (instances-1)*size, groupID)
		return err
	})
	if err == nil {
		s.summaryMu.Lock()
		s.summaryAt = time.Time{}
		s.summaryMu.Unlock()
	}
	return exists, err
}
