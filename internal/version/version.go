// Package version holds the build version and release-comparison helpers.
package version

import (
	"strconv"
	"strings"
)

// Version is stamped at build time by the release workflow via
// -ldflags "-X github.com/mophead64/deduper/internal/version.Version=v2026.09.14".
var Version = "dev"

// IsRelease reports whether v looks like a released tag (vYYYY.MM.DD[.N]).
func IsRelease(v string) bool {
	return len(parse(v)) > 0
}

// Newer reports whether candidate is a later release than current. It returns
// false if either isn't a parseable release tag (e.g. a "dev" build).
func Newer(candidate, current string) bool {
	a, b := parse(candidate), parse(current)
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	for i := 0; i < len(a) || i < len(b); i++ {
		var x, y int
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			return x > y
		}
	}
	return false
}

func parse(v string) []int {
	parts := strings.Split(strings.TrimPrefix(strings.TrimSpace(v), "v"), ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil
		}
		out = append(out, n)
	}
	return out
}
