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
	d := e.Sub(start).Round(time.Second)
	return d.String()
}
