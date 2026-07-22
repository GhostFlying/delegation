package broker

import "sync"

type mailboxKey struct {
	controllerID string
	treeID       string
	agentID      string
}

type mailboxNotifier struct {
	mu      sync.Mutex
	watches map[mailboxKey]*mailboxWatch
}

type mailboxWatch struct {
	wake    chan struct{}
	waiters int
}

type mailboxSubscription struct {
	notifier *mailboxNotifier
	key      mailboxKey
	watch    *mailboxWatch
	released sync.Once
}

func newMailboxNotifier() *mailboxNotifier {
	return &mailboxNotifier{watches: make(map[mailboxKey]*mailboxWatch)}
}

func (n *mailboxNotifier) subscribe(key mailboxKey) *mailboxSubscription {
	n.mu.Lock()
	defer n.mu.Unlock()
	watch := n.watches[key]
	if watch == nil {
		watch = &mailboxWatch{wake: make(chan struct{})}
		n.watches[key] = watch
	}
	watch.waiters++
	return &mailboxSubscription{notifier: n, key: key, watch: watch}
}

func (s *mailboxSubscription) channel() <-chan struct{} {
	return s.watch.wake
}

func (s *mailboxSubscription) release() {
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

func (n *mailboxNotifier) notify(key mailboxKey) {
	n.mu.Lock()
	watch := n.watches[key]
	delete(n.watches, key)
	n.mu.Unlock()
	if watch != nil {
		close(watch.wake)
	}
}
