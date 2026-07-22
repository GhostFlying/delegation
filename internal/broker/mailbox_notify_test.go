package broker

import "testing"

func TestMailboxNotifierReleasesIdleSubscriptionsAndWakesSharedWaiters(t *testing.T) {
	notifier := newMailboxNotifier()
	key := mailboxKey{
		controllerID: brokerTestControllerID,
		treeID:       brokerTestThreadID,
		agentID:      brokerMailboxWorkerAgentID,
	}
	first := notifier.subscribe(key)
	second := notifier.subscribe(key)
	first.release()
	if len(notifier.watches) != 1 || notifier.watches[key].waiters != 1 {
		t.Fatalf("notifier after first release = %#v", notifier.watches)
	}
	select {
	case <-second.channel():
		t.Fatal("releasing one subscription woke another waiter")
	default:
	}

	notifier.notify(key)
	select {
	case <-second.channel():
	default:
		t.Fatal("notifier did not wake the shared waiter")
	}
	second.release()
	if len(notifier.watches) != 0 {
		t.Fatalf("notifier retained a delivered subscription: %#v", notifier.watches)
	}

	idle := notifier.subscribe(key)
	idle.release()
	if len(notifier.watches) != 0 {
		t.Fatalf("notifier retained an idle subscription: %#v", notifier.watches)
	}
}
