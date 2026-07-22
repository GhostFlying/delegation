package broker

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestAgentOperationQueueSerializesEachAgentWithoutBlockingOthers(t *testing.T) {
	queue := newAgentOperationQueue()
	firstKey := agentOperationQueueKey{treeID: "tree-a", agentID: "agent-a"}
	secondKey := agentOperationQueueKey{treeID: "tree-a", agentID: "agent-b"}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	otherFinished := make(chan struct{})
	allFinished := make(chan struct{})
	var (
		mu    sync.Mutex
		order []string
	)
	record := func(value string) {
		mu.Lock()
		order = append(order, value)
		if len(order) == 3 {
			close(allFinished)
		}
		mu.Unlock()
	}
	queue.enqueue(firstKey, func() {
		close(firstStarted)
		<-releaseFirst
		record("first")
	})
	<-firstStarted
	queue.enqueue(firstKey, func() { record("second") })
	queue.enqueue(secondKey, func() {
		record("other")
		close(otherFinished)
	})
	select {
	case <-otherFinished:
	case <-time.After(time.Second):
		t.Fatal("independent agent operation was blocked")
	}
	close(releaseFirst)
	select {
	case <-allFinished:
	case <-time.After(time.Second):
		t.Fatal("serialized agent operations did not finish")
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"other", "first", "second"}) {
		t.Fatalf("operation order = %v", got)
	}
}
