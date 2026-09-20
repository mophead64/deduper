package web

import (
	"testing"
	"time"
)

func TestHumanDuration(t *testing.T) {
	cases := map[time.Duration]string{
		0:                 "0s",
		45 * time.Second:  "45s",
		192 * time.Second: "3m 12s",
		time.Hour:         "1h 0m 0s",
		2*time.Hour + 5*time.Minute + 12*time.Second: "2h 5m 12s",
		174404 * time.Second:                         "2d 0h 26m 44s",
	}
	for in, want := range cases {
		if got := humanDuration(in); got != want {
			t.Errorf("humanDuration(%v) = %q, want %q", in, got, want)
		}
	}
}
