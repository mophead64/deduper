package web

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/mophead64/deduper/internal/model"
)

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	roots, err := s.st.ListRoots(ctx)
	if err != nil {
		s.serverError(w, err)
		return
	}
	summary, err := s.st.SummaryStats(ctx)
	if err != nil {
		s.serverError(w, err)
		return
	}
	scans, err := s.st.ListScans(ctx, 5)
	if err != nil {
		s.serverError(w, err)
		return
	}
	current, err := s.st.CurrentScan(ctx)
	if err != nil {
		s.serverError(w, err)
		return
	}

	s.render(w, "dashboard.html", map[string]any{
		"Roots":       roots,
		"Summary":     summary,
		"RecentScans": scans,
		"Current":     current,
		"Running":     s.isRunning(),
	})
}

func (s *Server) renderRootsPanel(w http.ResponseWriter, r *http.Request) {
	roots, err := s.st.ListRoots(r.Context())
	if err != nil {
		s.serverError(w, err)
		return
	}
	s.render(w, "roots_panel.html", map[string]any{
		"Roots":   roots,
		"Running": s.isRunning(),
	})
}

func (s *Server) handleAddRoot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.badRequest(w, err)
		return
	}
	path := strings.TrimSpace(r.FormValue("path"))
	label := strings.TrimSpace(r.FormValue("label"))
	if path == "" {
		s.badRequest(w, errMsg("path is required"))
		return
	}
	if _, err := s.st.AddRoot(r.Context(), path, label, model.RootSourceManual); err != nil {
		s.badRequest(w, err)
		return
	}
	s.renderRootsPanel(w, r)
}

func (s *Server) handleDeleteRoot(w http.ResponseWriter, r *http.Request) {
	if s.isRunning() {
		s.badRequest(w, errMsg("cannot remove a root while a scan is running"))
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.badRequest(w, err)
		return
	}
	if err := s.st.DeleteRoot(r.Context(), id); err != nil {
		s.serverError(w, err)
		return
	}
	s.renderRootsPanel(w, r)
}

func (s *Server) handleToggleRoot(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.badRequest(w, err)
		return
	}
	root, err := s.st.GetRoot(r.Context(), id)
	if err != nil {
		s.badRequest(w, err)
		return
	}
	if err := s.st.SetRootEnabled(r.Context(), id, !root.Enabled); err != nil {
		s.serverError(w, err)
		return
	}
	s.renderRootsPanel(w, r)
}

type errMsg string

func (e errMsg) Error() string { return string(e) }

func (s *Server) badRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	s.render(w, "error_fragment.html", map[string]any{"Error": err.Error()})
}

func (s *Server) serverError(w http.ResponseWriter, err error) {
	s.log.Error("request failed", "error", err)
	w.WriteHeader(http.StatusInternalServerError)
	s.render(w, "error_fragment.html", map[string]any{"Error": "internal error"})
}
