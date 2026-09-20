package version

import "testing"

func TestNewer(t *testing.T) {
	cases := []struct {
		candidate, current string
		want               bool
	}{
		{"v2026.09.15", "v2026.09.14", true},
		{"v2026.09.14.1", "v2026.09.14", true},
		{"v2026.09.14", "v2026.09.14", false},
		{"v2026.09.14", "v2026.09.14.1", false},
		{"v2026.10.01", "v2026.09.30.4", true},
		{"v2026.09.14", "dev", false},
		{"garbage", "v2026.09.14", false},
	}
	for _, c := range cases {
		if got := Newer(c.candidate, c.current); got != c.want {
			t.Errorf("Newer(%q, %q) = %v, want %v", c.candidate, c.current, got, c.want)
		}
	}
}
