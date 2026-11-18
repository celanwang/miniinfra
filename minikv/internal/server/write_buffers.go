package server

import (
	"slices"
	"sync"
)

const (
	minReusableWriteBytes = 64 << 10
	maxIdleWriteBytes     = 8 << 20
	writeBufferStep       = 4 << 10
)

// writeBuffers retains only unpublished receive buffers, indexed by the
// backing capacity reserved from the instance's inbound budget.
type writeBuffers struct {
	mu    sync.Mutex
	idle  [][]byte
	bytes int64
	limit int64
}

func (p *writeBuffers) rounded(size, maxValue int) int {
	if size < minReusableWriteBytes {
		return size
	}
	if remainder := size % writeBufferStep; remainder != 0 {
		padding := writeBufferStep - remainder
		if padding <= maxValue-size && int64(size)+int64(padding) <= p.limit {
			return size + padding
		}
	}
	return size
}

func (p *writeBuffers) take(size int) []byte {
	if size >= minReusableWriteBytes && int64(size) <= p.limit {
		p.mu.Lock()
		for i, value := range p.idle {
			if len(value) == size {
				p.idle = slices.Delete(p.idle, i, i+1)
				p.bytes -= int64(size)
				p.mu.Unlock()
				return value
			}
		}
		p.mu.Unlock()
	}
	return make([]byte, size)
}

func (p *writeBuffers) put(value []byte) {
	size := int64(cap(value))
	if size < minReusableWriteBytes || size > p.limit {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Reclaim the oldest idle capacities so an inactive size cannot
	// keep a newly active size out of the pool.
	drop := 0
	for size > p.limit-p.bytes {
		p.bytes -= int64(cap(p.idle[drop]))
		drop++
	}
	p.idle = append(slices.Delete(p.idle, 0, drop), value)
	p.bytes += size
}
