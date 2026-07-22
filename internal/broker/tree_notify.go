package broker

import "sync"

type treeKey struct {
	controllerID string
	treeID       string
}

type treeNotifier struct {
	mu      sync.Mutex
	watches map[treeKey]*treeWatch
}

type treeWatch struct {
	wake    chan struct{}
	waiters int
}

type treeSubscription struct {
	notifier *treeNotifier
	key      treeKey
	watch    *treeWatch
	released sync.Once
}

func newTreeNotifier() *treeNotifier {
	return &treeNotifier{watches: make(map[treeKey]*treeWatch)}
}

func (n *treeNotifier) subscribe(key treeKey) *treeSubscription {
	n.mu.Lock()
	defer n.mu.Unlock()
	watch := n.watches[key]
	if watch == nil {
		watch = &treeWatch{wake: make(chan struct{})}
		n.watches[key] = watch
	}
	watch.waiters++
	return &treeSubscription{notifier: n, key: key, watch: watch}
}

func (s *treeSubscription) channel() <-chan struct{} {
	return s.watch.wake
}

func (s *treeSubscription) release() {
	s.released.Do(func() {
		s.notifier.mu.Lock()
		defer s.notifier.mu.Unlock()
		current := s.notifier.watches[s.key]
		if current != s.watch {
			return
		}
		current.waiters--
		if current.waiters == 0 {
			delete(s.notifier.watches, s.key)
		}
	})
}

func (n *treeNotifier) notify(key treeKey) {
	n.mu.Lock()
	watch := n.watches[key]
	delete(n.watches, key)
	n.mu.Unlock()
	if watch != nil {
		close(watch.wake)
	}
}
