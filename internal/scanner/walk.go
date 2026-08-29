package scanner

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// statEntry is the metadata pulled from a single regular file during the walk
// (Stage 0 in README.md). Symlinks and special files never reach this struct.
type statEntry struct {
	absPath string
	relPath string
	size    int64
	mtimeNs int64
	device  uint64
	inode   uint64
}

// walkResult reports one outcome of visiting a directory entry.
type walkResult struct {
	entry   *statEntry // non-nil for a regular file worth tracking
	skipped bool       // symlink, special file, or a stat/permission error
}

// walkRoot walks absRoot with up to `workers` directories being read
// concurrently, calling visit for every entry. visit is invoked from multiple
// goroutines, so it must be safe for concurrent use. It never follows symlinks
// (README.md §7.4) and never aborts for a single bad entry — permission errors
// and special files are reported via walkResult and the walk continues, per
// README.md §7. Traversal stops promptly once ctx is cancelled.
//
// filepath.WalkDir is single-threaded, which makes the walk latency-bound on
// readdir/stat on deep trees and network storage; fanning directory reads out
// keeps the disk (and, downstream, the DB writer) busy.
func walkRoot(ctx context.Context, absRoot string, workers int, visit func(walkResult)) error {
	if workers < 1 {
		workers = 1
	}
	// sem bounds how many directory reads run as their own goroutine; when it's
	// full a directory is walked inline on the current goroutine instead, so
	// traversal never stalls waiting for a slot.
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	var walk func(dir string)
	walk = func(dir string) {
		defer wg.Done()
		if ctx.Err() != nil {
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			// Permission error on the directory, or it vanished mid-walk.
			visit(walkResult{skipped: true})
			return
		}
		for _, d := range entries {
			if ctx.Err() != nil {
				return
			}
			full := filepath.Join(dir, d.Name())
			if d.IsDir() {
				wg.Add(1)
				select {
				case sem <- struct{}{}:
					go func(p string) {
						defer func() { <-sem }()
						walk(p)
					}(full)
				default:
					walk(full)
				}
				continue
			}
			visitFile(absRoot, full, d, visit)
		}
	}

	wg.Add(1)
	walk(absRoot)
	wg.Wait()
	return ctx.Err()
}

func visitFile(absRoot, path string, d fs.DirEntry, visit func(walkResult)) {
	if d.Type()&fs.ModeSymlink != 0 {
		visit(walkResult{skipped: true})
		return
	}
	if !d.Type().IsRegular() {
		// socket, device, FIFO, etc.
		visit(walkResult{skipped: true})
		return
	}
	info, err := d.Info()
	if err != nil {
		visit(walkResult{skipped: true})
		return
	}
	sys, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		visit(walkResult{skipped: true})
		return
	}
	rel, err := filepath.Rel(absRoot, path)
	if err != nil {
		visit(walkResult{skipped: true})
		return
	}
	visit(walkResult{entry: &statEntry{
		absPath: path,
		relPath: rel,
		size:    info.Size(),
		mtimeNs: info.ModTime().UnixNano(),
		device:  uint64(sys.Dev),
		inode:   uint64(sys.Ino),
	}})
}

// rootExists is a cheap upfront check so a bad/unmounted root path fails the
// scan with a clear error rather than silently walking zero files.
func rootExists(absRoot string) error {
	info, err := os.Stat(absRoot)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "stat", Path: absRoot, Err: os.ErrInvalid}
	}
	return nil
}
