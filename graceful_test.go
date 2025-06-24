package graceful_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nextlag/graceful"
)

var errShutdownFailed = errors.New("shutdown failed")

type mockSubscriber struct {
	onShutdownFunc func(ctx context.Context) error
	callCount      atomic.Int32
}

func (m *mockSubscriber) OnShutdown(ctx context.Context) error {
	m.callCount.Add(1)

	if m.onShutdownFunc != nil {
		return m.onShutdownFunc(ctx)
	}

	return nil
}

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		logger *slog.Logger
	}{
		{
			name:   "with logger",
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		},
		{
			name:   "with nil logger",
			logger: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			notifier := graceful.New(tt.logger)
			if notifier == nil {
				t.Fatal("expected non-nil ShutdownNotifier")
			}

			if notifier.SubscriberCount() != 0 {
				t.Error("expected 0 subscribers initially")
			}
		})
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	sub1 := &mockSubscriber{}
	sub2 := &mockSubscriber{}

	notifier.Register(sub1)

	if count := notifier.SubscriberCount(); count != 1 {
		t.Errorf("expected 1 subscriber, got %d", count)
	}

	notifier.Register(sub2)

	if count := notifier.SubscriberCount(); count != 2 {
		t.Errorf("expected 2 subscribers, got %d", count)
	}
}

func TestUnregister(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	sub1 := &mockSubscriber{}
	sub2 := &mockSubscriber{}
	sub3 := &mockSubscriber{}

	notifier.Register(sub1)
	notifier.Register(sub2)
	notifier.Register(sub3)

	if count := notifier.SubscriberCount(); count != 3 {
		t.Errorf("expected 3 subscribers, got %d", count)
	}

	notifier.Unregister(sub2)

	if count := notifier.SubscriberCount(); count != 2 {
		t.Errorf("expected 2 subscribers after unregister, got %d", count)
	}

	notifier.Shutdown()
	notifier.Wait()

	if sub1.callCount.Load() != 1 {
		t.Error("sub1 should have been called")
	}

	if sub2.callCount.Load() != 0 {
		t.Error("sub2 should not have been called")
	}

	if sub3.callCount.Load() != 1 {
		t.Error("sub3 should have been called")
	}
}

func TestUnregisterNonExistent(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	sub := &mockSubscriber{}

	notifier.Unregister(sub)

	if count := notifier.SubscriberCount(); count != 0 {
		t.Errorf("expected 0 subscribers, got %d", count)
	}
}

func TestShutdown(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	sub := &mockSubscriber{}
	notifier.Register(sub)

	go func() {
		time.Sleep(50 * time.Millisecond)
		notifier.Shutdown()
	}()

	notifier.Wait()

	if !notifier.IsShuttingDown() {
		t.Error("expected IsShuttingDown to be true")
	}

	if sub.callCount.Load() != 1 {
		t.Errorf("expected subscriber to be called once, got %d", sub.callCount.Load())
	}
}

func TestMultipleShutdownCalls(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	sub := &mockSubscriber{}
	notifier.Register(sub)

	var wg sync.WaitGroup

	for range 10 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			notifier.Shutdown()
		}()
	}

	wg.Wait()
	notifier.Wait()

	if sub.callCount.Load() != 1 {
		t.Errorf("expected subscriber to be called once despite multiple shutdowns, got %d", sub.callCount.Load())
	}
}

func TestContext(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)

	ctx1 := notifier.Context()
	ctx2 := notifier.Context()

	if ctx1 == ctx2 {
		t.Error("expected different contexts")
	}

	select {
	case <-ctx1.Done():
		t.Error("ctx1 should not be done")
	case <-ctx2.Done():
		t.Error("ctx2 should not be done")
	default:
	}

	notifier.Shutdown()
	notifier.Wait()

	select {
	case <-ctx1.Done():
	case <-time.After(100 * time.Millisecond):
		t.Error("ctx1 should be done after shutdown")
	}

	select {
	case <-ctx2.Done():
	case <-time.After(100 * time.Millisecond):
		t.Error("ctx2 should be done after shutdown")
	}
}

func TestDoneChannel(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	done := notifier.Done()

	select {
	case <-done:
		t.Error("done channel should not be closed")
	default:
	}

	notifier.Shutdown()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Error("done channel should be closed after shutdown")
	}
}

func TestIsShuttingDown(t *testing.T) {
	t.Parallel()

	shutdownNotifier := graceful.New(nil)

	if shutdownNotifier.IsShuttingDown() {
		t.Error("should not be shutting down initially")
	}

	shutdownNotifier.Shutdown()
	shutdownNotifier.Wait()

	if !shutdownNotifier.IsShuttingDown() {
		t.Error("should be shutting down after shutdown")
	}
}

func TestSubscriberError(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)

	errSub := &mockSubscriber{
		onShutdownFunc: func(_ context.Context) error {
			return errShutdownFailed
		},
	}

	successSub := &mockSubscriber{}

	notifier.Register(errSub)
	notifier.Register(successSub)

	notifier.Shutdown()
	notifier.Wait()

	if errSub.callCount.Load() != 1 {
		t.Error("error subscriber should have been called")
	}

	if successSub.callCount.Load() != 1 {
		t.Error("success subscriber should have been called despite error")
	}
}

func TestSubscriberTimeout(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)

	started := make(chan struct{})
	finished := make(chan struct{})

	slowSub := &mockSubscriber{
		onShutdownFunc: func(ctx context.Context) error {
			close(started)
			deadline, ok := ctx.Deadline()

			if !ok {
				t.Error("context should have deadline")
			}

			timeout := time.Until(deadline)
			if timeout < 29*time.Second || timeout > 31*time.Second {
				t.Errorf("expected timeout around 30s, got %v", timeout)
			}

			time.Sleep(10 * time.Millisecond)
			close(finished)

			return nil
		},
	}

	notifier.Register(slowSub)

	go func() {
		notifier.Shutdown()
	}()

	<-started

	<-finished

	done := make(chan struct{})

	go func() {
		notifier.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Error("Wait should complete quickly")
	}
}

func TestConcurrentRegisterUnregister(t *testing.T) {
	t.Parallel()

	notifier := graceful.New(nil)
	subscribers := make([]*mockSubscriber, 100)

	for i := range subscribers {
		subscribers[i] = &mockSubscriber{}
	}

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()
			notifier.Register(subscribers[idx])
		}(i)
	}

	for i := range 25 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			notifier.Unregister(subscribers[idx])
		}(i)
	}

	wg.Wait()

	expectedCount := 25
	if count := notifier.SubscriberCount(); count != expectedCount {
		t.Errorf("expected %d subscribers, got %d", expectedCount, count)
	}
}

//nolint:paralleltest // test has timing dependencies
func TestWaitBeforeShutdown(t *testing.T) {
	shutdownNotifier := graceful.New(nil)

	done := make(chan bool)

	go func() {
		shutdownNotifier.Wait()
		done <- true
	}()

	select {
	case <-done:
		t.Error("Wait should not return before shutdown")
	case <-time.After(100 * time.Millisecond):
	}

	shutdownNotifier.Shutdown()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Error("Wait should return after shutdown")
	}
}

func TestSignalHandling(t *testing.T) {
	t.Parallel()

	if os.Getenv("CI") != "" {
		t.Skip("Skipping signal test in CI environment")
	}

	notifier := graceful.New(nil)
	sub := &mockSubscriber{}
	notifier.Register(sub)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("failed to send signal: %v", err)
	}

	notifier.Wait()

	if !notifier.IsShuttingDown() {
		t.Error("expected shutdown after signal")
	}

	if sub.callCount.Load() != 1 {
		t.Errorf("expected subscriber to be called once, got %d", sub.callCount.Load())
	}
}
