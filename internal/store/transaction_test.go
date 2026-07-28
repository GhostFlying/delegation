package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

var transactionTestDriverSequence atomic.Uint64

func TestImmediateTransactionDiscardsConnectionAfterAmbiguousBegin(t *testing.T) {
	beginErr := errors.New("ambiguous begin")
	database, testDriver := openTransactionTestDatabase(t,
		transactionTestConnectionPlan{beginErr: beginErr},
		transactionTestConnectionPlan{},
	)

	operationCalled := false
	err := withImmediateTransaction(context.Background(), database, "test", func(*sql.Conn) error {
		operationCalled = true
		return nil
	})
	if !errors.Is(err, beginErr) || operationCalled {
		t.Fatalf("first transaction = %v, operationCalled=%v", err, operationCalled)
	}
	if err := withImmediateTransaction(
		context.Background(), database, "test", func(*sql.Conn) error { return nil },
	); err != nil {
		t.Fatalf("second transaction = %v", err)
	}
	if opened := testDriver.openedConnections(); opened != 2 {
		t.Fatalf("opened connections = %d, want 2", opened)
	}
}

func TestImmediateTransactionDiscardsConnectionAfterRollbackFailure(t *testing.T) {
	operationErr := errors.New("operation failed")
	rollbackErr := errors.New("rollback failed")
	database, testDriver := openTransactionTestDatabase(t,
		transactionTestConnectionPlan{rollbackErr: rollbackErr},
		transactionTestConnectionPlan{},
	)

	err := withImmediateTransaction(context.Background(), database, "test", func(*sql.Conn) error {
		return operationErr
	})
	if !errors.Is(err, operationErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("first transaction = %v", err)
	}
	if err := withImmediateTransaction(
		context.Background(), database, "test", func(*sql.Conn) error { return nil },
	); err != nil {
		t.Fatalf("second transaction = %v", err)
	}
	if opened := testDriver.openedConnections(); opened != 2 {
		t.Fatalf("opened connections = %d, want 2", opened)
	}
}

type transactionTestConnectionPlan struct {
	beginErr    error
	rollbackErr error
}

type transactionTestDriver struct {
	mu     sync.Mutex
	plans  []transactionTestConnectionPlan
	opened int
}

func openTransactionTestDatabase(
	t *testing.T,
	plans ...transactionTestConnectionPlan,
) (*sql.DB, *transactionTestDriver) {
	t.Helper()
	testDriver := &transactionTestDriver{plans: plans}
	name := fmt.Sprintf("delegation-transaction-test-%d", transactionTestDriverSequence.Add(1))
	sql.Register(name, testDriver)
	database, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	return database, testDriver
}

func (d *transactionTestDriver) Open(string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.opened >= len(d.plans) {
		return nil, errors.New("unexpected test connection")
	}
	plan := d.plans[d.opened]
	d.opened++
	return &transactionTestConnection{plan: plan}, nil
}

func (d *transactionTestDriver) openedConnections() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opened
}

type transactionTestConnection struct {
	plan          transactionTestConnectionPlan
	inTransaction bool
	beginAttempt  int
}

func (*transactionTestConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (*transactionTestConnection) Close() error {
	return nil
}

func (*transactionTestConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("driver transactions are not supported")
}

func (c *transactionTestConnection) ExecContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Result, error) {
	switch query {
	case "BEGIN IMMEDIATE":
		if c.inTransaction {
			return nil, errors.New("cannot start a transaction within a transaction")
		}
		c.inTransaction = true
		if c.beginAttempt == 0 && c.plan.beginErr != nil {
			c.beginAttempt++
			return nil, c.plan.beginErr
		}
	case "COMMIT":
		if !c.inTransaction {
			return nil, errors.New("cannot commit: no transaction is active")
		}
		c.inTransaction = false
	case "ROLLBACK":
		if c.plan.rollbackErr != nil {
			return nil, c.plan.rollbackErr
		}
		c.inTransaction = false
	default:
		return nil, fmt.Errorf("unexpected query %q", query)
	}
	return driver.RowsAffected(0), nil
}
