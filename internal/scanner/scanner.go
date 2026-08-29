// Package scanner implements the staged duplicate-detection pipeline
// described in README.md §6: walk & stat, size filter, partial hash, full hash,
// then a materialized regroup that collapses hardlinks.
package scanner

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mophead64/deduper/internal/model"
	"github.com/mophead64/deduper/internal/store"
)

// ProgressFunc receives live progress events while a scan runs.
type ProgressFunc func(model.ProgressEvent)

// Batch sizes for the walk and hash pipelines. Big enough to amortise the
// per-transaction cost that made the old row-at-a-time writes the bottleneck,
// small enough that WAL checkpoints still run between commits (so the WAL file
// doesn't grow without bound) and memory stays flat regardless of tree size.
const (
	walkBatchSize = 2000
	hashPageSize  = 4096
	hashBatchSize = 500
)

// Options tunes the scanner's resource usage.
type Options struct {
	// WalkWorkers bounds concurrent directory reads during the walk phase.
	// readdir/stat latency (not bandwidth) dominates on deep trees and network
	// storage, so oversubscribing CPUs here is deliberate.
	WalkWorkers int
	// HashWorkers bounds the partial/full hashing pool. SHA-256 over multi-TB
	// is genuinely CPU-heavy, so this scales with cores rather than sitting at
	// a fixed small number.
	HashWorkers int
}

func DefaultOptions() Options {
	n := runtime.NumCPU()
	return Options{
		WalkWorkers: clamp(n*2, 4, 16),
		HashWorkers: clamp(n, 4, 12),
	}
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

type Scanner struct {
	st   *store.Store
	opts Options
}

func New(st *store.Store, opts Options) *Scanner {
	if opts.WalkWorkers < 1 {
		opts.WalkWorkers = 1
	}
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

	c.setPhase(model.PhaseHashing)
	if err := s.hashPhase(ctx, c); err != nil {
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

// walkPhase walks each root concurrently and streams the results into a single
// batching writer. The walk is classified entirely in memory against a
// preloaded snapshot of the root's known files, so:
//   - no SELECT-per-file (the old FindFile round trip is gone),
//   - unchanged files cost zero writes on a rescan,
//   - "missing" is a set difference computed locally rather than a per-root
//     UPDATE scanning the whole table.
func (s *Scanner) walkPhase(ctx context.Context, scanID int64, roots []model.Root, c *counters) error {
	for _, root := range roots {
		if err := rootExists(root.Path); err != nil {
			return fmt.Errorf("root %s: %w", root.Path, err)
		}
		if err := s.walkOneRoot(ctx, scanID, root, c); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (s *Scanner) walkOneRoot(ctx context.Context, scanID int64, root model.Root, c *counters) error {
	existing, err := s.st.LoadRootFiles(ctx, root.ID)
	if err != nil {
		return fmt.Errorf("load existing files for %s: %w", root.Path, err)
	}

	// A child context so a writer failure stops the walkers promptly.
	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan walkResult, 8192)
	var consErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		consErr = s.consumeWalk(walkCtx, scanID, root, existing, results, c)
		if consErr != nil {
			cancel()
		}
	}()

	// walkRoot blocks until every walker goroutine has finished, so it's safe
	// to close(results) only after it returns.
	walkErr := walkRoot(walkCtx, root.Path, s.opts.WalkWorkers, func(wr walkResult) {
		select {
		case results <- wr:
		case <-walkCtx.Done():
		}
	})
	close(results)
	<-done

	if ctx.Err() != nil {
		return ctx.Err() // scan cancelled: report that, not the fallout from it
	}
	if consErr != nil {
		return fmt.Errorf("persist walk of %s: %w", root.Path, consErr)
	}
	if walkErr != nil {
		return fmt.Errorf("walk %s: %w", root.Path, walkErr)
	}
	return nil
}

// consumeWalk is the single writer for one root's walk. It owns the batch
// slices (no locking needed) and the `existing` map, deleting each entry it
// sees so whatever remains at the end is exactly the set of now-missing files.
func (s *Scanner) consumeWalk(ctx context.Context, scanID int64, root model.Root, existing map[string]store.ExistingFile, results <-chan walkResult, c *counters) error {
	var (
		inserts   []store.NewFile
		changes   []store.ChangedFile
		reappear  []int64
		pending   int
		processed int
	)

	flush := func() error {
		if err := s.st.InsertFilesBatch(ctx, root.ID, scanID, inserts); err != nil {
			return err
		}
		if err := s.st.UpdateFilesChangedBatch(ctx, scanID, changes); err != nil {
			return err
		}
		if err := s.st.MarkFilesPresentBatch(ctx, scanID, reappear); err != nil {
			return err
		}
		inserts, changes, reappear, pending = inserts[:0], changes[:0], reappear[:0], 0
		return nil
	}

	for wr := range results {
		if wr.skipped {
			c.skipped.Add(1)
			continue
		}
		e := wr.entry
		c.seen.Add(1)
		processed++
		if processed&127 == 0 {
			c.setPath(e.absPath)
		}

		if x, ok := existing[e.relPath]; ok {
			delete(existing, e.relPath)
			switch {
			case x.Size != e.size || x.MTimeNs != e.mtimeNs || x.Device != e.device || x.Inode != e.inode:
				changes = append(changes, store.ChangedFile{
					ID: x.ID, Size: e.size, MTimeNs: e.mtimeNs, Device: e.device, Inode: e.inode,
				})
				c.changed.Add(1)
				pending++
			case x.Missing:
				// Reappeared unchanged: clear the missing flag, keep the hashes.
				reappear = append(reappear, x.ID)
				pending++
			default:
				// Unchanged and present: nothing to write.
			}
		} else {
			inserts = append(inserts, store.NewFile{
				RelPath: e.relPath, Size: e.size, MTimeNs: e.mtimeNs, Device: e.device, Inode: e.inode,
			})
			c.added.Add(1)
			pending++
		}

		if pending >= walkBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// Anything still in `existing` was recorded before but not seen this walk.
	var missing []int64
	for _, x := range existing {
		if !x.Missing {
			missing = append(missing, x.ID)
		}
	}
	if err := s.st.MarkFilesMissingBatch(ctx, missing); err != nil {
		return err
	}
	c.removed.Add(int64(len(missing)))
	return nil
}

// hashResult is one worker's output for the batching consumer.
type hashResult struct {
	id    int64
	hash  string
	bytes int64
	err   bool
}

// hashStage bundles the per-stage differences (Stage 2 partial vs Stage 4 full)
// so runHashStage itself stays generic.
type hashStage struct {
	name    string
	page    func(ctx context.Context, afterID int64, limit int) ([]store.HashCandidate, error)
	persist func(ctx context.Context, results []store.HashResult) error
	hash    func(abs string, size int64) (hash string, bytesRead int64, err error)
}

func (s *Scanner) hashPhase(ctx context.Context, c *counters) error {
	rootPaths, err := s.st.AllRootPaths(ctx)
	if err != nil {
		return fmt.Errorf("resolve root paths: %w", err)
	}

	stages := []hashStage{
		{
			name:    "partial",
			page:    s.st.PartialHashCandidatesPage,
			persist: s.st.UpdatePartialHashesBatch,
			hash: func(abs string, size int64) (string, int64, error) {
				h, err := partialHash(abs, size)
				return h, min64(size, sampleSize*3), err
			},
		},
		{
			name:    "full",
			page:    s.st.FullHashCandidatesPage,
			persist: s.st.UpdateFullHashesBatch,
			hash: func(abs string, size int64) (string, int64, error) {
				h, err := fullHash(abs)
				return h, size, err
			},
		},
	}
	for _, st := range stages {
		if err := s.runHashStage(ctx, c, rootPaths, st); err != nil {
			return err
		}
	}
	return nil
}

// runHashStage streams candidate pages into a bounded worker pool and batches
// the resulting hashes back to the store. Candidates are paged (not loaded into
// one big slice, and not held open as a long-lived cursor) so memory stays flat
// and the reader connection is released between pages.
func (s *Scanner) runHashStage(ctx context.Context, c *counters, rootPaths map[int64]string, st hashStage) error {
	stageCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan store.HashCandidate, 256)
	results := make(chan hashResult, 256)

	prodErr := make(chan error, 1)
	go func() { prodErr <- feedHashJobs(stageCtx, cancel, st.page, jobs) }()

	var workers sync.WaitGroup
	for i := 0; i < s.opts.HashWorkers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			hashWorker(stageCtx, rootPaths, st.hash, jobs, results, c)
		}()
	}
	go func() { workers.Wait(); close(results) }()

	batchErr := s.persistHashResults(ctx, cancel, st.persist, results, c)
	// results is closed only after every worker exits, which happens only after
	// jobs is closed by feedHashJobs — so its error is now waiting.
	feedErr := <-prodErr

	switch {
	case batchErr != nil:
		return fmt.Errorf("persist %s hashes: %w", st.name, batchErr)
	case feedErr != nil:
		return fmt.Errorf("list %s hash candidates: %w", st.name, feedErr)
	default:
		return ctx.Err()
	}
}

// feedHashJobs pages candidates (id > afterID, ascending) into jobs until a page
// comes back empty. Paging keeps a processed row out of later pages because the
// hash it writes takes the row out of the candidate predicate.
func feedHashJobs(ctx context.Context, cancel context.CancelFunc, page func(context.Context, int64, int) ([]store.HashCandidate, error), jobs chan<- store.HashCandidate) error {
	defer close(jobs)
	var afterID int64
	for {
		candidates, err := page(ctx, afterID, hashPageSize)
		if err != nil {
			cancel()
			return err
		}
		if len(candidates) == 0 {
			return nil
		}
		for _, cand := range candidates {
			select {
			case jobs <- cand:
			case <-ctx.Done():
				return nil
			}
		}
		afterID = candidates[len(candidates)-1].ID
	}
}

func hashWorker(ctx context.Context, rootPaths map[int64]string, hash func(string, int64) (string, int64, error), jobs <-chan store.HashCandidate, results chan<- hashResult, c *counters) {
	for cand := range jobs {
		if ctx.Err() != nil {
			continue
		}
		abs := filepath.Join(rootPaths[cand.RootID], cand.RelPath)
		c.setPath(abs)
		h, n, err := hash(abs, cand.Size)
		if err != nil {
			results <- hashResult{err: true}
			continue
		}
		results <- hashResult{id: cand.ID, hash: h, bytes: n}
	}
}

func (s *Scanner) persistHashResults(ctx context.Context, cancel context.CancelFunc, persist func(context.Context, []store.HashResult) error, results <-chan hashResult, c *counters) error {
	var batch []store.HashResult
	var err error
	flush := func() {
		if err != nil || len(batch) == 0 {
			return
		}
		if e := persist(ctx, batch); e != nil {
			err = e
			cancel()
			return
		}
		batch = batch[:0]
	}
	for r := range results {
		if r.err {
			c.skipped.Add(1)
			continue
		}
		c.bytesHashed.Add(r.bytes)
		batch = append(batch, store.HashResult{ID: r.id, Hash: r.hash})
		if len(batch) >= hashBatchSize {
			flush()
		}
	}
	flush()
	return err
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
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
