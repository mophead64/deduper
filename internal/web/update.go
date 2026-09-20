package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/mophead64/deduper/internal/version"
)

const (
	latestReleaseURL = "https://api.github.com/repos/mophead64/deduper/releases/latest"
	updateOKTTL      = time.Hour
	updateErrTTL     = 5 * time.Minute
)

type updateInfo struct {
	Available bool
	Latest    string
	URL       string
	Checked   bool // false if the check failed (offline, rate limited, ...)
}

// updateChecker asks GitHub for the latest release, caching the answer so
// dashboard loads don't each hit the API (unauthenticated calls are rate limited).
type updateChecker struct {
	client *http.Client
	url    string

	mu     sync.Mutex
	info   updateInfo
	expiry time.Time
}

func newUpdateChecker() *updateChecker {
	return &updateChecker{client: &http.Client{Timeout: 5 * time.Second}, url: latestReleaseURL}
}

func (u *updateChecker) check(ctx context.Context) updateInfo {
	u.mu.Lock()
	defer u.mu.Unlock()
	if time.Now().Before(u.expiry) {
		return u.info
	}

	info, err := u.fetch(ctx)
	if err != nil {
		u.info, u.expiry = updateInfo{}, time.Now().Add(updateErrTTL)
		return u.info
	}
	u.info, u.expiry = info, time.Now().Add(updateOKTTL)
	return info
}

func (u *updateChecker) fetch(ctx context.Context) (updateInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.url, nil)
	if err != nil {
		return updateInfo{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "deduper/"+version.Version)
	resp, err := u.client.Do(req)
	if err != nil {
		return updateInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return updateInfo{}, fmt.Errorf("github returned %s", resp.Status)
	}
	var rel struct {
		TagName string `json:"tag_name"`
		HTMLURL string `json:"html_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return updateInfo{}, err
	}
	return updateInfo{
		Checked:   true,
		Latest:    rel.TagName,
		URL:       rel.HTMLURL,
		Available: version.Newer(rel.TagName, version.Version),
	}, nil
}

// handleVersionCheck is fetched by htmx after the dashboard renders, so the
// network round trip to GitHub never delays the page itself.
func (s *Server) handleVersionCheck(w http.ResponseWriter, r *http.Request) {
	s.render(w, "version_status.html", map[string]any{
		"Version": version.Version,
		"Update":  s.updates.check(r.Context()),
	})
}
