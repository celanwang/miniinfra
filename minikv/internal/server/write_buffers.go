package server

import "sync"

const (
	minReusableWriteBytes = 64 << 10
	maxIdleWriteBytes     = 8 << 20
)

// writeBuffers retains only unpublished receive buffers. Each retained
// capacity is exact, so reuse never increases a stored value's byte charge.
type writeBuffers struct {
	mu    sync.Mutex
	idle  [][]byte
	bytes int64
	limit int64
}

func (p *writeBuffers) take(size int) []byte {
	if size >= minReusableWriteBytes && int64(size) <= p.limit {
		p.mu.Lock()
		for i, value := range p.idle {
			if len(value) == size {
				last := len(p.idle) - 1
				p.idle[i] = p.idle[last]
				p.idle[last] = nil
				p.idle = p.idle[:last]
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
	if size <= p.limit-p.bytes {
		p.idle = append(p.idle, value)
		p.bytes += size
	}
}
