package appserver

import "sync"

type tailBuffer struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit, data: make([]byte, 0, limit)}
}

func (b *tailBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(data) >= b.limit {
		b.data = append(b.data[:0], data[len(data)-b.limit:]...)
		return len(data), nil
	}
	overflow := len(b.data) + len(data) - b.limit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, data...)
	return len(data), nil
}

func (b *tailBuffer) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}
