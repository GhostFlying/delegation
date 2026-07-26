package appserver

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCallMarksDefinitivePreWriteFailures(t *testing.T) {
	t.Run("empty method", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		err := client.Call(context.Background(), "", nil, nil)
		assertRequestNotWritten(t, err, nil)
		assertNoWrites(t, writer)
	})

	t.Run("encode", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		err := client.Call(
			context.Background(), "turn/start", map[string]any{"unsupported": make(chan struct{})}, nil,
		)
		assertRequestNotWritten(t, err, nil)
		assertNoWrites(t, writer)
	})

	t.Run("oversize", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		err := client.Call(
			context.Background(), "turn/start", strings.Repeat("x", MaxMessageBytes), nil,
		)
		assertRequestNotWritten(t, err, ErrMessageTooLarge)
		assertNoWrites(t, writer)
	})

	t.Run("writer acquisition canceled", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		<-client.writeGate
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := client.Call(ctx, "turn/start", nil, nil)
		assertRequestNotWritten(t, err, context.Canceled)
		assertNoWrites(t, writer)
	})

	t.Run("closed", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		<-client.writeGate
		close(client.done)
		err := client.Call(context.Background(), "turn/start", nil, nil)
		assertRequestNotWritten(t, err, ErrClosed)
		assertNoWrites(t, writer)
	})

	t.Run("busy", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		client.pending[99] = pendingCall{result: make(chan response, 1)}
		err := client.Call(context.Background(), "turn/start", nil, nil)
		assertRequestNotWritten(t, err, ErrBusy)
		assertNoWrites(t, writer)
	})
}

func TestCallClassifiesSynchronousWriteReceipts(t *testing.T) {
	wantErr := errors.New("injected write failure")
	for _, test := range []struct {
		name       string
		write      func([]byte) (int, error)
		notWritten bool
	}{
		{
			name: "zero bytes with error",
			write: func([]byte) (int, error) {
				return 0, wantErr
			},
			notWritten: true,
		},
		{
			name: "zero bytes without error",
			write: func([]byte) (int, error) {
				return 0, nil
			},
			notWritten: true,
		},
		{
			name: "partial bytes with error",
			write: func(data []byte) (int, error) {
				return min(1, len(data)), wantErr
			},
		},
		{
			name: "complete bytes with error",
			write: func(data []byte) (int, error) {
				return len(data), wantErr
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, writer := newWriteClassificationClient()
			writer.write = test.write
			err := client.Call(context.Background(), "turn/start", nil, nil)
			if test.notWritten != errors.Is(err, ErrRequestNotWritten) {
				t.Fatalf("Call() error = %v, not-written = %t", err, test.notWritten)
			}
			if test.name == "zero bytes without error" {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("Call() error = %v, want io.ErrShortWrite", err)
				}
			} else if !errors.Is(err, wantErr) {
				t.Fatalf("Call() error = %v, want injected failure", err)
			}
		})
	}
}

func TestCallWriteTimeoutHasUnknownReceipt(t *testing.T) {
	writer := newBlockingWriteCloser()
	client, _ := newWriteClassificationClientWith(writer)
	client.writeTimeout = 20 * time.Millisecond

	err := client.Call(context.Background(), "turn/start", nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call() error = %v, want deadline exceeded", err)
	}
	if errors.Is(err, ErrRequestNotWritten) {
		t.Fatalf("write timeout was incorrectly classified as not written: %v", err)
	}
	select {
	case <-writer.entered:
	default:
		t.Fatal("blocking writer was not called")
	}
}

func TestCallDoesNotMarkFailuresAfterWrite(t *testing.T) {
	t.Run("response wait timeout", func(t *testing.T) {
		client, _ := newWriteClassificationClient()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := client.Call(ctx, "turn/start", nil, nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call() error = %v, want deadline exceeded", err)
		}
		if errors.Is(err, ErrRequestNotWritten) {
			t.Fatalf("response timeout was incorrectly classified as not written: %v", err)
		}
	})

	t.Run("RPC error response", func(t *testing.T) {
		client, writer := newWriteClassificationClient()
		writer.write = func(data []byte) (int, error) {
			if err := client.complete(decodedMessage{
				responseID: 1,
				rpcError:   &RPCError{Code: -32602, Message: "invalid params"},
			}); err != nil {
				t.Errorf("complete RPC error: %v", err)
			}
			return len(data), nil
		}
		err := client.Call(context.Background(), "turn/start", nil, nil)
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != -32602 {
			t.Fatalf("Call() error = %T %v, want RPCError", err, err)
		}
		if errors.Is(err, ErrRequestNotWritten) {
			t.Fatalf("RPC rejection was incorrectly classified as not written: %v", err)
		}
	})
}

func TestWriteAllReportsReceiptAndByteCount(t *testing.T) {
	wantErr := errors.New("injected write failure")
	result := writeAll(writerFunc(func(data []byte) (int, error) {
		return min(2, len(data)), wantErr
	}), []byte("request"))
	if result.receipt != writeReceiptPartial || result.written != 2 || !errors.Is(result.err, wantErr) {
		t.Fatalf("partial write result = %#v", result)
	}

	result = writeAll(writerFunc(func(data []byte) (int, error) {
		return len(data), nil
	}), []byte("request"))
	if result != (writeResult{receipt: writeReceiptComplete, written: 7}) {
		t.Fatalf("complete write result = %#v", result)
	}
}

func assertRequestNotWritten(t *testing.T, err, cause error) {
	t.Helper()
	if !errors.Is(err, ErrRequestNotWritten) {
		t.Fatalf("error = %v, want ErrRequestNotWritten", err)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("error = %v, want cause %v", err, cause)
	}
}

func assertNoWrites(t *testing.T, writer *classificationWriteCloser) {
	t.Helper()
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.calls != 0 {
		t.Fatalf("writer received %d calls before definitive failure", writer.calls)
	}
}

func newWriteClassificationClient() (*Client, *classificationWriteCloser) {
	return newWriteClassificationClientWith(&classificationWriteCloser{})
}

func newWriteClassificationClientWith(
	stdin io.WriteCloser,
) (*Client, *classificationWriteCloser) {
	writer, _ := stdin.(*classificationWriteCloser)
	writeGate := make(chan struct{}, 1)
	writeGate <- struct{}{}
	return &Client{
		processOwner: &gatedOwnedProcess{},
		stdin:        stdin,
		writeTimeout: 100 * time.Millisecond,
		maxPending:   1,
		writeGate:    writeGate,
		pending:      make(map[uint64]pendingCall),
		abandoned:    make(map[uint64]struct{}),
		fatal:        make(chan error, 1),
		done:         make(chan struct{}),
	}, writer
}

type classificationWriteCloser struct {
	mu    sync.Mutex
	calls int
	write func([]byte) (int, error)
}

func (w *classificationWriteCloser) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.calls++
	write := w.write
	w.mu.Unlock()
	if write != nil {
		return write(data)
	}
	return len(data), nil
}

func (*classificationWriteCloser) Close() error {
	return nil
}

type blockingWriteCloser struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockingWriteCloser() *blockingWriteCloser {
	return &blockingWriteCloser{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (w *blockingWriteCloser) Write([]byte) (int, error) {
	close(w.entered)
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockingWriteCloser) Close() error {
	w.once.Do(func() { close(w.closed) })
	return nil
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(data []byte) (int, error) {
	return f(data)
}
