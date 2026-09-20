package scanner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/mophead64/deduper/internal/store"
)

// GroupRescan summarises what RescanGroup found.
type GroupRescan struct {
	Checked   int
	Missing   int  // gone, or no longer a regular file
	Changed   int  // stat data moved and the file was re-hashed
	Errors    int  // could not be checked (permissions, I/O); left untouched
	Exists    bool // false if the group dissolved (fewer than two members left)
	Unchanged int
}

// RescanGroup re-checks just the files of one duplicate group against the
// filesystem and updates the current state in place. It is the cleanup-time
// counterpart to a full scan: no scan record is created, other files and
// groups are untouched, and it never walks a directory.
//
// A file whose size, mtime, device or inode moved is re-hashed straight away
// rather than trusted, so replacing a duplicate with a hardlink to the keeper
// (new inode, same content) keeps it in the group while an edited file drops
// out.
func (s *Scanner) RescanGroup(ctx context.Context, groupID int64) (GroupRescan, error) {
	var res GroupRescan
	if _, err := s.st.GetDuplicateGroup(ctx, groupID); err != nil {
		return res, fmt.Errorf("load group: %w", err)
	}
	members, err := s.st.GetDuplicateGroupMembers(ctx, groupID)
	if err != nil {
		return res, fmt.Errorf("load group members: %w", err)
	}

	var missing []int64
	var updates []store.GroupRescanUpdate
	for _, m := range members {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		res.Checked++
		abs := filepath.Join(m.RootPath, m.RelPath)

		// Lstat: a file swapped for a symlink is no longer tracked, matching the walk.
		info, err := os.Lstat(abs)
		if err != nil {
			if os.IsNotExist(err) {
				missing = append(missing, m.FileID)
				res.Missing++
			} else {
				res.Errors++
			}
			continue
		}
		sys, ok := info.Sys().(*syscall.Stat_t)
		if !info.Mode().IsRegular() || !ok {
			missing = append(missing, m.FileID)
			res.Missing++
			continue
		}

		u := store.GroupRescanUpdate{
			ID: m.FileID, Size: info.Size(), MTimeNs: info.ModTime().UnixNano(),
			Device: uint64(sys.Dev), Inode: uint64(sys.Ino),
		}
		if u.Size == m.Size && u.MTimeNs == m.MTimeNs && u.Device == m.Device && u.Inode == m.Inode {
			res.Unchanged++
			continue
		}
		if u.Hash, err = fullHash(abs); err != nil {
			res.Errors++
			continue
		}
		updates = append(updates, u)
		res.Changed++
	}

	res.Exists, err = s.st.ApplyGroupRescan(ctx, groupID, missing, updates)
	return res, err
}
