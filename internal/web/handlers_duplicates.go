package web

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/mophead64/deduper/internal/model"
	"github.com/mophead64/deduper/internal/store"
)

func (s *Server) handleDuplicates(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := store.DuplicateFilter{
		Page:     1,
		PageSize: 25,
	}
	if v := q.Get("root_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.RootID = id
		}
	}
	if v := q.Get("min_size"); v != "" {
		if sz, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.MinSize = sz
		}
	}
	filter.NameLike = q.Get("q")
	if v := q.Get("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			filter.Page = p
		}
	}

	groups, total, err := s.st.ListDuplicateGroups(r.Context(), filter)
	if err != nil {
		s.serverError(w, err)
		return
	}

	roots, err := s.st.ListRoots(r.Context())
	if err != nil {
		s.serverError(w, err)
		return
	}

	totalPages := (total + filter.PageSize - 1) / filter.PageSize
	if totalPages == 0 {
		totalPages = 1
	}

	data := map[string]any{
		"Groups":     groups,
		"Total":      total,
		"Page":       filter.Page,
		"TotalPages": totalPages,
		"RootID":     filter.RootID,
		"MinSize":    filter.MinSize,
		"Query":      filter.NameLike,
		"Roots":      roots,
		"Resolved":   q.Get("resolved") != "",
	}

	if r.Header.Get("HX-Request") == "true" {
		s.render(w, "duplicates_results.html", data)
		return
	}
	s.render(w, "duplicates.html", data)
}

func (s *Server) handleDuplicateDetail(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.badRequest(w, err)
		return
	}
	group, err := s.st.GetDuplicateGroup(r.Context(), id)
	if err != nil {
		s.badRequest(w, errMsg("duplicate group not found"))
		return
	}
	members, err := s.st.GetDuplicateGroupMembers(r.Context(), id)
	if err != nil {
		s.serverError(w, err)
		return
	}

	type instanceKey struct {
		device, inode uint64
	}
	counts := map[instanceKey]int{}
	for _, m := range members {
		counts[instanceKey{m.Device, m.Inode}]++
	}
	type memberView struct {
		Member      model.DuplicateGroupMember
		SharesInode bool
	}
	views := make([]memberView, len(members))
	for i, m := range members {
		views[i] = memberView{Member: m, SharesInode: counts[instanceKey{m.Device, m.Inode}] > 1}
	}

	s.render(w, "duplicate_detail.html", map[string]any{
		"Group":   group,
		"Members": views,
		"Rescan":  rescanNotice(r.URL.Query()),
	})
}

// handleRescanGroup re-checks one group's files on disk and updates the current
// state in place (no scan record). It redirects back with the outcome in the
// query string, or to the list if the group dissolved.
func (s *Server) handleRescanGroup(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.badRequest(w, err)
		return
	}
	if s.st.Indexing() || s.isRunning() {
		s.badRequest(w, errMsg("a scan is in progress; wait for it to finish before rescanning a group"))
		return
	}
	res, err := s.sc.RescanGroup(r.Context(), id)
	if err != nil {
		s.serverError(w, err)
		return
	}
	if r.FormValue("from") == "list" {
		// Back to the list with its filters and page, taken from the Referer
		// (same-origin /duplicates only) so the redirect can't go elsewhere.
		back := url.Values{}
		if ref, err := url.Parse(r.Referer()); err == nil && ref.Path == "/duplicates" {
			back = ref.Query()
		}
		if !res.Exists {
			back.Set("resolved", "1")
		} else {
			back.Del("resolved")
		}
		http.Redirect(w, r, "/duplicates?"+back.Encode(), http.StatusSeeOther)
		return
	}
	if !res.Exists {
		http.Redirect(w, r, "/duplicates?resolved=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/duplicates/%d?rescanned=1&missing=%d&changed=%d&errors=%d",
		id, res.Missing, res.Changed, res.Errors), http.StatusSeeOther)
}

type rescanView struct {
	Missing, Changed, Errors int
}

// rescanNotice decodes the outcome handleRescanGroup put in the redirect URL,
// or returns nil if the page wasn't reached via a rescan.
func rescanNotice(q url.Values) *rescanView {
	if q.Get("rescanned") == "" {
		return nil
	}
	n := func(k string) int { v, _ := strconv.Atoi(q.Get(k)); return v }
	return &rescanView{Missing: n("missing"), Changed: n("changed"), Errors: n("errors")}
}
