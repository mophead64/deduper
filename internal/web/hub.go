package web

import (
	"sync"

	"github.com/mophead64/deduper/internal/model"
)

// hub fans out progress events for the currently running scan to any number
// of SSE subscribers (browser tabs watching the dashboard).
type hub struct {
	mu   sync.Mutex
	subs map[chan model.ProgressEvent]struct{}
}

func newHub() *hub {
	return &hub{subs: make(map[chan model.ProgressEvent]struct{})}
}

func (h *hub) subscribe() chan model.ProgressEvent {
	ch := make(chan model.ProgressEvent, 16)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan model.ProgressEvent) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
	close(ch)
}

// publish is the scanner.ProgressFunc passed into Scanner.Run. Slow or absent
// subscribers never block a scan: a full channel just drops the event, since
// the next tick supersedes it anyway.
func (h *hub) publish(ev model.ProgressEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}
