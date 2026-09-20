// Command deduper runs the Deduper web application: it serves the dashboard
// and duplicate-detection API described in README.md, backed by a SQLite
// database at DEDUPER_DB (default /data/deduper.db).
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/mophead64/deduper/internal/scanner"
	"github.com/mophead64/deduper/internal/store"
	"github.com/mophead64/deduper/internal/version"
	"github.com/mophead64/deduper/internal/web"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dbPath := getenv("DEDUPER_DB", "/data/deduper.db")
	addr := ":" + getenv("PORT", "8080")
	scanBase := getenv("DEDUPER_SCAN_BASE", "/scan")

	// A soft memory ceiling lets the GC stay aggressive under a big scan instead
	// of letting the heap (and RSS) drift up. GOMEMLIMIT is also honoured by the
	// runtime directly; this env var is a convenience that takes MiB.
	if v := os.Getenv("DEDUPER_MEMORY_LIMIT_MIB"); v != "" {
		if mib, err := strconv.ParseInt(v, 10, 64); err == nil && mib > 0 {
			debug.SetMemoryLimit(mib << 20)
			log.Info("soft memory limit set", "mib", mib)
		} else {
			log.Warn("ignoring invalid DEDUPER_MEMORY_LIMIT_MIB", "value", v)
		}
	}

	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("failed to create db directory", "dir", dir, "error", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting", "version", version.Version, "db", dbPath)
	t0 := time.Now()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		log.Error("failed to open database", "path", dbPath, "error", err)
		os.Exit(1)
	}
	defer st.Close()
	log.Info("database opened", "elapsed", time.Since(t0).Round(time.Millisecond).String())

	if err := st.MarkInterruptedScans(ctx); err != nil {
		log.Error("failed to reconcile interrupted scans", "error", err)
	}

	if added, err := st.DiscoverRoots(ctx, scanBase); err != nil {
		log.Error("root auto-discovery failed", "base", scanBase, "error", err)
	} else if len(added) > 0 {
		log.Info("auto-discovered roots", "base", scanBase, "added", added)
	}

	go func() {
		if err := st.EnsureIndexes(ctx, log); err != nil && ctx.Err() == nil {
			log.Error("index build failed", "error", err)
		}
	}()

	scanOpts := scanner.DefaultOptions()
	if n := getenvInt(log, "DEDUPER_WALK_WORKERS"); n > 0 {
		scanOpts.WalkWorkers = n
	}
	if n := getenvInt(log, "DEDUPER_HASH_WORKERS"); n > 0 {
		scanOpts.HashWorkers = n
	}
	log.Info("scanner concurrency", "walk_workers", scanOpts.WalkWorkers, "hash_workers", scanOpts.HashWorkers)
	sc := scanner.New(st, scanOpts)

	srv, err := web.NewServer(st, sc, log)
	if err != nil {
		log.Error("failed to initialize web server", "error", err)
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful shutdown failed", "error", err)
		}
	}()

	log.Info("deduper listening", "addr", addr, "db", dbPath)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server error", "error", err)
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getenvInt reads a positive integer env var, warning (and returning 0, i.e.
// "use the default") on a non-numeric or non-positive value.
func getenvInt(log *slog.Logger, key string) int {
	v := os.Getenv(key)
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		log.Warn("ignoring invalid worker count", "var", key, "value", v)
		return 0
	}
	return n
}
