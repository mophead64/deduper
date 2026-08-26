package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mophead64/deduper/internal/model"
)

// DiscoverRoots looks for immediate subdirectories of basePath (the
// convention is one read-only bind mount per root under /scan/<name>, see
// README.md) and registers any not already known as an enabled root, labeled
// with the directory name. It's meant to be called once at startup so a
// container brought up with new bind mounts picks them up without a manual
// trip through the UI.
//
// basePath not existing is normal outside Docker (or before any mounts are
// added) and is not an error: it just means nothing to discover.
func (s *Store) DiscoverRoots(ctx context.Context, basePath string) ([]string, error) {
	entries, err := os.ReadDir(basePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", basePath, err)
	}

	existing, err := s.ListRoots(ctx)
	if err != nil {
		return nil, fmt.Errorf("list existing roots: %w", err)
	}
	known := make(map[string]bool, len(existing))
	for _, r := range existing {
		known[r.Path] = true
	}

	var added []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(basePath, entry.Name())
		if known[path] {
			continue
		}
		if _, err := s.AddRoot(ctx, path, entry.Name(), model.RootSourceAuto); err != nil {
			return added, fmt.Errorf("add discovered root %s: %w", path, err)
		}
		added = append(added, path)
	}
	return added, nil
}
