package broker

import "sync"

type agentOperationQueueKey struct {
	treeID  string
	agentID string
}

type agentOperationQueue struct {
	mu      sync.Mutex
	pending map[agentOperationQueueKey][]func()
}

func newAgentOperationQueue() *agentOperationQueue {
	return &agentOperationQueue{pending: make(map[agentOperationQueueKey][]func())}
}

func (q *agentOperationQueue) enqueue(key agentOperationQueueKey, operation func()) {
	q.mu.Lock()
	q.pending[key] = append(q.pending[key], operation)
	start := len(q.pending[key]) == 1
	q.mu.Unlock()
	if start {
		go q.run(key)
	}
}

func (q *agentOperationQueue) run(key agentOperationQueueKey) {
	for {
		q.mu.Lock()
		operations := q.pending[key]
		if len(operations) == 0 {
			delete(q.pending, key)
			q.mu.Unlock()
			return
		}
		operation := operations[0]
		q.mu.Unlock()

		operation()

		q.mu.Lock()
		operations = q.pending[key]
		if len(operations) == 1 {
			delete(q.pending, key)
			q.mu.Unlock()
			return
		}
		q.pending[key] = operations[1:]
		q.mu.Unlock()
	}
}
