package graceful_test

import (
	"context"
	"errors"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nextlag/graceful"
)

var (
	errShutdownFailed = errors.New("shutdown failed")
	errParentStopped  = errors.New("parent stopped")
)

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

func newNotifier(t *testing.T, opts ...graceful.Option) (*graceful.ShutdownNotifier, context.Context) {
	t.Helper()

	notifier, ctx := graceful.New(t.Context(), append([]graceful.Option{graceful.WithSignals()}, opts...)...)

	t.Cleanup(func() {
		notifier.Shutdown()
		waitShutdown(t, notifier)
	})

	return notifier, ctx
}

func waitShutdown(t *testing.T, notifier *graceful.ShutdownNotifier) {
	t.Helper()

	err := notifier.Wait()
	if err != nil {
		t.Errorf("Wait() error = %v", err)
	}
}

func TestRegisterAndUnregister(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)
		kept, removed := &mockSubscriber{}, &mockSubscriber{}

		notifier.Register(kept)
		notifier.Register(removed)

		if got := notifier.SubscriberCount(); got != 2 {
			t.Fatalf("SubscriberCount() = %d, want 2", got)
		}

		notifier.Unregister(removed)
		notifier.Unregister(&mockSubscriber{})

		if got := notifier.SubscriberCount(); got != 1 {
			t.Fatalf("SubscriberCount() after Unregister = %d, want 1", got)
		}

		notifier.Shutdown()

		if err := notifier.Wait(); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}

		if got := kept.callCount.Load(); got != 1 {
			t.Errorf("kept subscriber calls = %d, want 1", got)
		}

		if got := removed.callCount.Load(); got != 0 {
			t.Errorf("removed subscriber calls = %d, want 0", got)
		}
	})
}

func TestShutdownNotifiesSubscribersOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)
		sub := &mockSubscriber{}
		notifier.Register(sub)

		for range 10 {
			go notifier.Shutdown()
		}

		waitShutdown(t, notifier)
		notifier.Shutdown()

		if got := sub.callCount.Load(); got != 1 {
			t.Errorf("subscriber calls = %d, want 1", got)
		}
	})
}

func TestWaitBlocksUntilShutdown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)

		var returned atomic.Bool

		go func() {
			waitShutdown(t, notifier)

			returned.Store(true)
		}()

		synctest.Wait()

		if returned.Load() {
			t.Fatal("Wait() returned before Shutdown()")
		}

		notifier.Shutdown()
		synctest.Wait()

		if !returned.Load() {
			t.Fatal("Wait() did not return after Shutdown()")
		}
	})
}

func TestStateWhileSubscribersRun(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, ctx := newNotifier(t)

		var (
			shuttingDown, done bool
			ctxErr             error
		)

		notifier.Register(&mockSubscriber{onShutdownFunc: func(context.Context) error {
			shuttingDown = notifier.IsShuttingDown()
			ctxErr = ctx.Err()

			select {
			case <-notifier.Done():
				done = true
			default:
			}

			return nil
		}})

		if notifier.IsShuttingDown() || ctx.Err() != nil {
			t.Fatal("notifier is shutting down before Shutdown()")
		}

		notifier.Shutdown()
		waitShutdown(t, notifier)

		if !shuttingDown {
			t.Error("IsShuttingDown() = false inside OnShutdown, want true")
		}

		if !errors.Is(ctxErr, context.Canceled) {
			t.Errorf("ctx.Err() inside OnShutdown = %v, want context.Canceled", ctxErr)
		}

		if done {
			t.Error("Done() closed inside OnShutdown, want closed after subscribers")
		}
	})
}

func TestSubscriberMayCallNotifier(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)

		notifier.Register(&mockSubscriber{onShutdownFunc: func(context.Context) error {
			notifier.Register(&mockSubscriber{})
			notifier.SubscriberCount()

			return nil
		}})

		notifier.Shutdown()
		waitShutdown(t, notifier)
	})
}

func TestWaitReturnsSubscriberErrors(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := graceful.New(t.Context(), graceful.WithSignals())
		failing := &mockSubscriber{onShutdownFunc: func(context.Context) error { return errShutdownFailed }}
		succeeding := &mockSubscriber{}

		notifier.Register(failing)
		notifier.Register(succeeding)

		notifier.Shutdown()

		err := notifier.Wait()
		if !errors.Is(err, errShutdownFailed) {
			t.Errorf("Wait() error = %v, want %v", err, errShutdownFailed)
		}

		if got := succeeding.callCount.Load(); got != 1 {
			t.Errorf("subscriber after failing one calls = %d, want 1", got)
		}
	})
}

func TestTimeoutIsSharedBySubscribers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []graceful.Option
		want time.Duration
	}{
		{name: "default", want: 30 * time.Second},
		{name: "WithTimeout", opts: []graceful.Option{graceful.WithTimeout(5 * time.Second)}, want: 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				notifier, _ := newNotifier(t, tt.opts...)
				start := time.Now()

				var (
					gotTimeout time.Duration
					secondErr  error
				)

				notifier.Register(&mockSubscriber{onShutdownFunc: func(ctx context.Context) error {
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Error("OnShutdown ctx has no deadline")
					}

					gotTimeout = deadline.Sub(start)

					<-ctx.Done()

					return nil
				}})
				notifier.Register(&mockSubscriber{onShutdownFunc: func(ctx context.Context) error {
					secondErr = ctx.Err()

					return nil
				}})

				notifier.Shutdown()
				waitShutdown(t, notifier)

				if gotTimeout != tt.want {
					t.Errorf("timeout = %v, want %v", gotTimeout, tt.want)
				}

				if !errors.Is(secondErr, context.DeadlineExceeded) {
					t.Errorf("second subscriber ctx.Err() = %v, want context.DeadlineExceeded", secondErr)
				}
			})
		})
	}
}

func TestShutdownCause(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, ctx := newNotifier(t)

		notifier.Shutdown()
		<-ctx.Done()

		if got := context.Cause(ctx); !errors.Is(got, graceful.ErrShutdownRequested) {
			t.Errorf("context.Cause() = %v, want %v", got, graceful.ErrShutdownRequested)
		}
	})
}

func TestParentCancelStartsShutdown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancelCause(t.Context())
		notifier, ctx := graceful.New(parent, graceful.WithSignals())
		sub := &mockSubscriber{}
		notifier.Register(sub)

		cancel(errParentStopped)
		waitShutdown(t, notifier)

		if got := sub.callCount.Load(); got != 1 {
			t.Errorf("subscriber calls = %d, want 1", got)
		}

		if got := context.Cause(ctx); !errors.Is(got, errParentStopped) {
			t.Errorf("context.Cause() = %v, want %v", got, errParentStopped)
		}
	})
}

func TestDefaultSignalStartsShutdown(t *testing.T) {
	notifier, ctx := graceful.New(t.Context())
	sub := &mockSubscriber{}
	notifier.Register(sub)

	err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	if err != nil {
		t.Fatalf("Kill() error = %v", err)
	}

	waitShutdown(t, notifier)

	if got := sub.callCount.Load(); got != 1 {
		t.Errorf("subscriber calls = %d, want 1", got)
	}

	var signalErr graceful.SignalError
	if !errors.As(context.Cause(ctx), &signalErr) || signalErr.Signal != syscall.SIGUSR1 {
		t.Errorf("context.Cause() = %v, want SignalError{SIGUSR1}", context.Cause(ctx))
	}

	if got, want := signalErr.Error(), "received signal "+syscall.SIGUSR1.String(); got != want {
		t.Errorf("SignalError.Error() = %q, want %q", got, want)
	}
}

func TestIgnoredSignalStaysIgnored(t *testing.T) {
	signal.Ignore(syscall.SIGHUP)
	t.Cleanup(func() { signal.Reset(syscall.SIGHUP) })

	notifier, _ := graceful.New(t.Context(), graceful.WithSignals(syscall.SIGHUP))

	if !signal.Ignored(syscall.SIGHUP) {
		t.Error("New() stopped ignoring SIGHUP, want it to stay ignored")
	}

	notifier.Shutdown()
	waitShutdown(t, notifier)
}
