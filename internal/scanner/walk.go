package scanner

import (
	"io/fs"
	"os"
	"path/filepath"
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

// walkRoot walks absRoot depth-first, calling visit for every entry. It never
// follows symlinks (README.md §7.4) and never returns an error for a single bad
// entry — permission errors and special files are reported via walkResult and
// the walk continues, per README.md §7 ("permission errors ... scan continues").
func walkRoot(absRoot string, visit func(walkResult)) error {
	return filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Typically a permission error opening a directory, or a race
			// where the entry vanished mid-walk. Skip it, keep walking.
			visit(walkResult{skipped: true})
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			visit(walkResult{skipped: true})
			return nil
		}
		if !d.Type().IsRegular() {
			// socket, device, FIFO, etc.
			visit(walkResult{skipped: true})
			return nil
		}

		info, err := d.Info()
		if err != nil {
			visit(walkResult{skipped: true})
			return nil
		}
		sys, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			visit(walkResult{skipped: true})
			return nil
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			visit(walkResult{skipped: true})
			return nil
		}

		visit(walkResult{entry: &statEntry{
			absPath: path,
			relPath: rel,
			size:    info.Size(),
			mtimeNs: info.ModTime().UnixNano(),
			device:  uint64(sys.Dev),
			inode:   uint64(sys.Ino),
		}})
		return nil
	})
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
