// Package scanner implements the staged duplicate-detection pipeline
// described in README.md §6: walk & stat, size filter, partial hash, full hash,
// then a materialized regroup that collapses hardlinks.
package scanner

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mophead64/deduper/internal/model"
	"github.com/mophead64/deduper/internal/store"
)

// ProgressFunc receives live progress events while a scan runs.
type ProgressFunc func(model.ProgressEvent)

// Options tunes the scanner's resource usage.
type Options struct {
	HashWorkers int // bounded worker pool for partial/full hashing; I/O bound, keep modest
}

func DefaultOptions() Options {
	return Options{HashWorkers: 4}
}

type Scanner struct {
	st   *store.Store
	opts Options
}

func New(st *store.Store, opts Options) *Scanner {
	if opts.HashWorkers < 1 {
		opts.HashWorkers = 1
	}
	return &Scanner{st: st, opts: opts}
}

// counters is updated atomically from the walk and hashing goroutines and
// read periodically by the progress ticker, so the UI gets live numbers
// without every single file update round-tripping to SQLite or the SSE hub.
type counters struct {
	seen, added, changed, removed, skipped, bytesHashed atomic.Int64
	currentPath                                         atomic.Value // string
	phase                                               atomic.Value // string
}

func (c *counters) setPath(p string) { c.currentPath.Store(p) }
func (c *counters) path() string {
	v, _ := c.currentPath.Load().(string)
	return v
}

func (c *counters) setPhase(p model.ScanPhase) { c.phase.Store(string(p)) }
func (c *counters) getPhase() model.ScanPhase {
	v, _ := c.phase.Load().(string)
	return model.ScanPhase(v)
}

// Run executes one full scan of the given roots: walk, hash, rebuild
// duplicate groups. It updates the scans row throughout and is safe to run
// in its own goroutine; onProgress is called from that goroutine's context
// only (never concurrently with itself).
func (s *Scanner) Run(ctx context.Context, scanID int64, roots []model.Root, onProgress ProgressFunc) error {
	c := &counters{}
	c.setPath("")
	c.setPhase(model.PhaseWalking)
	start := time.Now()

	stopTicker := s.startProgressTicker(ctx, scanID, c, start, onProgress)
	defer stopTicker()

	if err := s.walkPhase(ctx, scanID, roots, c); err != nil {
		return err
	}
	if err := s.st.UpdateScanCounters(ctx, scanID, model.PhaseWalking,
		c.seen.Load(), c.added.Load(), c.changed.Load(), c.removed.Load(), c.skipped.Load(), c.bytesHashed.Load()); err != nil {
		return fmt.Errorf("update counters after walk: %w", err)
	}

	rootIDs := make([]int64, len(roots))
	for i, r := range roots {
		rootIDs[i] = r.ID
	}
	removed, err := s.st.MarkMissing(ctx, rootIDs, scanID)
	if err != nil {
		return fmt.Errorf("mark missing: %w", err)
	}
	c.removed.Store(removed)

	c.setPhase(model.PhaseHashing)
	if err := s.hashPhase(ctx, scanID, c); err != nil {
		return err
	}

	c.setPhase(model.PhaseFinalizing)
	if err := s.st.UpdateScanCounters(ctx, scanID, model.PhaseFinalizing,
		c.seen.Load(), c.added.Load(), c.changed.Load(), c.removed.Load(), c.skipped.Load(), c.bytesHashed.Load()); err != nil {
		return err
	}
	if onProgress != nil {
		onProgress(s.snapshot(scanID, model.PhaseFinalizing, c, start))
	}

	if err := s.st.RebuildDuplicateGroups(ctx); err != nil {
		return fmt.Errorf("rebuild duplicate groups: %w", err)
	}

	return nil
}

func (s *Scanner) walkPhase(ctx context.Context, scanID int64, roots []model.Root, c *counters) error {
	for _, root := range roots {
		if err := rootExists(root.Path); err != nil {
			return fmt.Errorf("root %s: %w", root.Path, err)
		}
		err := walkRoot(root.Path, func(wr walkResult) {
			if ctx.Err() != nil {
				return
			}
			if wr.skipped {
				c.skipped.Add(1)
				return
			}
			e := wr.entry
			c.seen.Add(1)
			c.setPath(filepath.Join(root.Path, e.relPath))

			existing, err := s.st.FindFile(ctx, root.ID, e.relPath)
			if err != nil {
				c.skipped.Add(1)
				return
			}
			if existing == nil {
				if _, err := s.st.InsertFile(ctx, root.ID, e.relPath, e.size, e.mtimeNs, e.device, e.inode, scanID); err != nil {
					c.skipped.Add(1)
					return
				}
				c.added.Add(1)
				return
			}
			if existing.Size != e.size || existing.MTimeNs != e.mtimeNs || existing.Device != e.device || existing.Inode != e.inode {
				if err := s.st.UpdateFileChanged(ctx, existing.ID, e.size, e.mtimeNs, e.device, e.inode, scanID); err != nil {
					c.skipped.Add(1)
					return
				}
				c.changed.Add(1)
				return
			}
			// Unchanged: reuse stored hashes (README.md §6.1), just mark seen.
			if err := s.st.TouchFileSeen(ctx, existing.ID, scanID); err != nil {
				c.skipped.Add(1)
			}
		})
		if err != nil {
			return fmt.Errorf("walk %s: %w", root.Path, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (s *Scanner) hashPhase(ctx context.Context, scanID int64, c *counters) error {
	partial, err := s.st.FilesNeedingPartialHash(ctx)
	if err != nil {
		return fmt.Errorf("list partial-hash candidates: %w", err)
	}
	if err := s.runHashWorkers(ctx, partial, c, func(cand store.HashCandidate, rootPath string) error {
		abs := filepath.Join(rootPath, cand.RelPath)
		h, err := partialHash(abs, cand.Size)
		if err != nil {
			return err
		}
		c.bytesHashed.Add(min64(cand.Size, sampleSize*3))
		return s.st.UpdateFilePartialHash(ctx, cand.ID, h)
	}); err != nil {
		return err
	}

	full, err := s.st.FilesNeedingFullHash(ctx)
	if err != nil {
		return fmt.Errorf("list full-hash candidates: %w", err)
	}
	return s.runHashWorkers(ctx, full, c, func(cand store.HashCandidate, rootPath string) error {
		abs := filepath.Join(rootPath, cand.RelPath)
		h, err := fullHash(abs)
		if err != nil {
			return err
		}
		c.bytesHashed.Add(cand.Size)
		return s.st.UpdateFileFullHash(ctx, cand.ID, h)
	})
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// runHashWorkers fans candidates out across a bounded pool. A single
// candidate's failure (permission error, file vanished, etc.) is counted as
// skipped and does not abort the batch, matching README.md §7's "continue on
// per-file error" policy.
func (s *Scanner) runHashWorkers(ctx context.Context, cands []store.HashCandidate, c *counters, work func(store.HashCandidate, string) error) error {
	if len(cands) == 0 {
		return nil
	}

	rootPaths := map[int64]string{}
	for _, cand := range cands {
		if _, ok := rootPaths[cand.RootID]; !ok {
			p, err := s.st.RootPath(ctx, cand.RootID)
			if err != nil {
				return fmt.Errorf("resolve root path: %w", err)
			}
			rootPaths[cand.RootID] = p
		}
	}

	jobs := make(chan store.HashCandidate)
	var wg sync.WaitGroup
	for i := 0; i < s.opts.HashWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cand := range jobs {
				if ctx.Err() != nil {
					continue
				}
				c.setPath(filepath.Join(rootPaths[cand.RootID], cand.RelPath))
				if err := work(cand, rootPaths[cand.RootID]); err != nil {
					c.skipped.Add(1)
				}
			}
		}()
	}
	for _, cand := range cands {
		jobs <- cand
	}
	close(jobs)
	wg.Wait()
	return ctx.Err()
}

func (s *Scanner) startProgressTicker(ctx context.Context, scanID int64, c *counters, start time.Time, onProgress ProgressFunc) func() {
	if onProgress == nil {
		return func() { /* nothing to stop */ }
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				onProgress(s.snapshot(scanID, c.getPhase(), c, start))
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

func (s *Scanner) snapshot(scanID int64, phase model.ScanPhase, c *counters, start time.Time) model.ProgressEvent {
	return model.ProgressEvent{
		Type:         "progress",
		ScanID:       scanID,
		Phase:        string(phase),
		FilesSeen:    c.seen.Load(),
		FilesNew:     c.added.Load(),
		FilesChanged: c.changed.Load(),
		FilesRemoved: c.removed.Load(),
		FilesSkipped: c.skipped.Load(),
		BytesHashed:  c.bytesHashed.Load(),
		CurrentPath:  c.path(),
		ElapsedMs:    time.Since(start).Milliseconds(),
	}
}
