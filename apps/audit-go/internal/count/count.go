package count

import "sync"

// Counter tracks completed runs per workflow. Safe for concurrent use.
type Counter struct {
	mu     sync.RWMutex
	counts map[string]int
}

func New() *Counter {
	return &Counter{counts: make(map[string]int)}
}

func (c *Counter) Add(workflowID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[workflowID]++
}

// Snapshot returns a copy of the current counts.
func (c *Counter) Snapshot() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}
