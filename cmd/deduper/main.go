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
	"syscall"
	"time"

	"github.com/mophead64/deduper/internal/scanner"
	"github.com/mophead64/deduper/internal/store"
	"github.com/mophead64/deduper/internal/web"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dbPath := getenv("DEDUPER_DB", "/data/deduper.db")
	addr := ":" + getenv("PORT", "8080")
	scanBase := getenv("DEDUPER_SCAN_BASE", "/scan")

	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("failed to create db directory", "dir", dir, "error", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		log.Error("failed to open database", "path", dbPath, "error", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.MarkInterruptedScans(ctx); err != nil {
		log.Error("failed to reconcile interrupted scans", "error", err)
	}

	if added, err := st.DiscoverRoots(ctx, scanBase); err != nil {
		log.Error("root auto-discovery failed", "base", scanBase, "error", err)
	} else if len(added) > 0 {
		log.Info("auto-discovered roots", "base", scanBase, "added", added)
	}

	sc := scanner.New(st, scanner.DefaultOptions())

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
