package web

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/mophead64/deduper/internal/model"
)

func (s *Server) handleStartScan(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.badRequest(w, err)
		return
	}

	allRoots, err := s.st.ListRoots(r.Context())
	if err != nil {
		s.serverError(w, err)
		return
	}

	var targets []model.Root
	rootIDParam := r.FormValue("root_id")
	if rootIDParam == "" || rootIDParam == "all" {
		for _, root := range allRoots {
			if root.Enabled {
				targets = append(targets, root)
			}
		}
	} else {
		id, err := strconv.ParseInt(rootIDParam, 10, 64)
		if err != nil {
			s.badRequest(w, err)
			return
		}
		for _, root := range allRoots {
			if root.ID == id && root.Enabled {
				targets = append(targets, root)
			}
		}
	}

	if len(targets) == 0 {
		s.badRequest(w, errMsg("no enabled roots to scan"))
		return
	}

	scanID, err := s.startScan(targets)
	if err != nil {
		s.badRequest(w, err)
		return
	}

	current, err := s.st.CurrentScan(r.Context())
	if err != nil {
		s.serverError(w, err)
		return
	}
	if current == nil || current.ID != scanID {
		// A handful of files can finish scanning before this handler even
		// builds its response — CurrentScan (status='running') no longer
		// finds it. There's no live panel to show and no EventSource will
		// ever be opened for it, so nothing would otherwise tell the page
		// to pick up the result: force the reload directly instead of
		// leaving the dashboard showing stale pre-scan numbers.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<div id="scan-panel"><script>window.location.reload();</script></div>`)
		return
	}

	s.renderScanPanel(w, r)
}

func (s *Server) renderScanPanel(w http.ResponseWriter, r *http.Request) {
	current, err := s.st.CurrentScan(r.Context())
	if err != nil {
		s.serverError(w, err)
		return
	}
	roots, err := s.st.ListRoots(r.Context())
	if err != nil {
		s.serverError(w, err)
		return
	}
	s.render(w, "scan_panel.html", map[string]any{"Current": current, "Roots": roots})
}

func (s *Server) handleCurrentScan(w http.ResponseWriter, r *http.Request) {
	s.renderScanPanel(w, r)
}

// handleScanEvents streams live progress for one scan via SSE (README.md §8,
// §10). It forwards hub events matching this scan's id and closes the stream
// once a terminal event for that id arrives.
func (s *Server) handleScanEvents(w http.ResponseWriter, r *http.Request) {
	scanID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid scan id", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	bw := bufio.NewWriter(w)
	ctx := r.Context()

	// Subscribe before checking the scan's persisted status: if a scan
	// finishes in the gap between the check and the subscribe, it would
	// otherwise broadcast its terminal event to a hub nobody is listening to
	// yet, and this connection would wait forever for an event that's
	// already been and gone.
	sub := s.hub.subscribe()
	defer s.hub.unsubscribe(sub)

	// A fast scan can start and finish before the browser's EventSource
	// connection is even established — the POST that started it has to
	// round-trip and get swapped into the DOM first. If that already
	// happened, replay the terminal state from the persisted scan record
	// instead of waiting on a hub event that was already broadcast to no
	// one.
	if scan, err := s.st.GetScan(ctx, scanID); err == nil && scan.Status != model.ScanRunning {
		writeSSE(bw, flusher, terminalEventFromScan(scan))
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub:
			if !ok {
				return
			}
			if ev.ScanID != scanID {
				continue
			}
			writeSSE(bw, flusher, ev)
			if ev.Type == "scan_completed" || ev.Type == "scan_failed" {
				return
			}
		}
	}
}

func writeSSE(bw *bufio.Writer, flusher http.Flusher, ev model.ProgressEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		return
	}
	fmt.Fprintf(bw, "event: %s\ndata: %s\n\n", ev.Type, payload)
	bw.Flush()
	flusher.Flush()
}

// terminalEventFromScan reconstructs the progress event a finished scan
// would have broadcast, for a client that connects after the fact.
func terminalEventFromScan(scan model.Scan) model.ProgressEvent {
	evType := "scan_completed"
	if scan.Status == model.ScanFailed || scan.Status == model.ScanInterrupted {
		evType = "scan_failed"
	}
	var elapsedMs int64
	if scan.FinishedAt != nil {
		elapsedMs = scan.FinishedAt.Sub(scan.StartedAt).Milliseconds()
	}
	return model.ProgressEvent{
		Type:         evType,
		ScanID:       scan.ID,
		Phase:        string(scan.Phase),
		FilesSeen:    scan.FilesSeen,
		FilesNew:     scan.FilesNew,
		FilesChanged: scan.FilesChanged,
		FilesRemoved: scan.FilesRemoved,
		FilesSkipped: scan.FilesSkipped,
		BytesHashed:  scan.BytesHashed,
		Error:        scan.Error,
		ElapsedMs:    elapsedMs,
	}
}

func (s *Server) handleScanHistory(w http.ResponseWriter, r *http.Request) {
	scans, err := s.st.ListScans(r.Context(), 50)
	if err != nil {
		s.serverError(w, err)
		return
	}
	s.render(w, "scan_history.html", map[string]any{"Scans": scans})
}
