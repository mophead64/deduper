// Package web serves the dashboard, duplicates browser, and scan API/SSE
// stream described in README.md §9-10, server-rendered with htmx.
package web

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sync"

	"github.com/mophead64/deduper/internal/model"
	"github.com/mophead64/deduper/internal/scanner"
	"github.com/mophead64/deduper/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Server struct {
	st  *store.Store
	sc  *scanner.Scanner
	hub *hub
	log *slog.Logger

	tmpl    *template.Template
	updates *updateChecker

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
}

func NewServer(st *store.Store, sc *scanner.Scanner, log *slog.Logger) (*Server, error) {
	tmpl, err := template.New("").Funcs(templateFuncs).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	return &Server{st: st, sc: sc, hub: newHub(), log: log, tmpl: tmpl, updates: newUpdateChecker()}, nil
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /static/", http.FileServerFS(staticFS))

	mux.HandleFunc("GET /{$}", s.handleDashboard)

	mux.HandleFunc("GET /version/check", s.handleVersionCheck)

	mux.HandleFunc("POST /roots", s.handleAddRoot)
	mux.HandleFunc("POST /roots/{id}/delete", s.handleDeleteRoot)
	mux.HandleFunc("POST /roots/{id}/toggle", s.handleToggleRoot)

	mux.HandleFunc("POST /scans", s.handleStartScan)
	mux.HandleFunc("GET /scans/current", s.handleCurrentScan)
	mux.HandleFunc("GET /scans/{id}/events", s.handleScanEvents)
	mux.HandleFunc("GET /scans", s.handleScanHistory)

	mux.HandleFunc("GET /duplicates", s.handleDuplicates)
	mux.HandleFunc("GET /duplicates/{id}", s.handleDuplicateDetail)
	mux.HandleFunc("POST /duplicates/{id}/rescan", s.handleRescanGroup)

	return mux
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		s.log.Error("template render failed", "template", name, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (s *Server) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// startScan launches a scan in the background if one isn't already running.
// It owns the running flag for the lifetime of the scan. Scanner.Run only
// rebuilds the materialized duplicate view once walking and hashing finish
// cleanly, so a scan that fails partway through leaves the last-known-good
// duplicate data in place rather than replacing it with partial results.
func (s *Server) startScan(roots []model.Root) (int64, error) {
	if s.st.Indexing() {
		return 0, fmt.Errorf("a one-time database index build is in progress; try again in a few minutes (see the app log)")
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return 0, fmt.Errorf("a scan is already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.running = true
	s.cancel = cancel
	s.mu.Unlock()

	rootIDs := make([]int64, len(roots))
	for i, r := range roots {
		rootIDs[i] = r.ID
	}
	scanID, err := s.st.StartScan(ctx, rootIDs)
	if err != nil {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		cancel()
		return 0, err
	}

	s.hub.publish(model.ProgressEvent{Type: "scan_started", ScanID: scanID})

	go func() {
		defer func() {
			s.mu.Lock()
			s.running = false
			s.cancel = nil
			s.mu.Unlock()
			cancel()
		}()

		runErr := s.sc.Run(ctx, scanID, roots, s.hub.publish)

		status := model.ScanCompleted
		errMsg := ""
		evType := "scan_completed"
		if runErr != nil {
			status = model.ScanFailed
			errMsg = runErr.Error()
			evType = "scan_failed"
			s.log.Error("scan failed", "scan_id", scanID, "error", runErr)
		}
		if err := s.st.FinishScan(context.Background(), scanID, status, errMsg); err != nil {
			s.log.Error("failed to finalize scan record", "scan_id", scanID, "error", err)
		}
		s.hub.publish(model.ProgressEvent{Type: evType, ScanID: scanID, Error: errMsg})

		// A scan builds large transient structures (the per-root file snapshot,
		// candidate pages). Hand that memory back to the OS now rather than
		// letting RSS sit high until the next GC cycle happens to release it.
		debug.FreeOSMemory()
	}()

	return scanID, nil
}
