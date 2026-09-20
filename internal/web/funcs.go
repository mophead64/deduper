package web

import (
	"fmt"
	"html/template"
	"time"
)

var templateFuncs = template.FuncMap{
	"bytes":     formatBytes,
	"time":      formatTime,
	"shorthash": shortHash,
	"duration":  formatDuration,
	"add":       func(a, b int) int { return a + b },
	"sub":       func(a, b int) int { return a - b },
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n2 := n / unit; n2 >= unit; n2 /= unit {
		div *= unit
		exp++
	}
	units := "KMGTPE"
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), units[exp])
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func shortHash(h string) string {
	if len(h) <= 12 {
		return h
	}
	return h[:12]
}

func formatDuration(start time.Time, end *time.Time) string {
	e := time.Now()
	if end != nil {
		e = *end
	}
	return humanDuration(e.Sub(start))
}

// humanDuration renders 45s, 3m 12s, 2h 5m 12s, or 1d 4h 5m 12s. The JS in
// scan_panel.html mirrors this format for the live elapsed counter.
func humanDuration(d time.Duration) string {
	s := int64(d.Round(time.Second) / time.Second)
	if s < 0 {
		s = 0
	}
	days, hours, mins, secs := s/86400, s%86400/3600, s%3600/60, s%60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh %dm %ds", days, hours, mins, secs)
	case hours > 0:
		return fmt.Sprintf("%dh %dm %ds", hours, mins, secs)
	case mins > 0:
		return fmt.Sprintf("%dm %ds", mins, secs)
	default:
		return fmt.Sprintf("%ds", secs)
	}
}
