package graceful_test

import (
	"context"
	"errors"
	"os"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"weak"

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

type subscriberFunc func(ctx context.Context) error

func (f subscriberFunc) OnShutdown(ctx context.Context) error {
	return f(ctx)
}

type ctxKey struct{}

func newNotifier(t *testing.T, opts ...graceful.Option) (*graceful.ShutdownNotifier, context.Context) {
	t.Helper()

	notifier, ctx := graceful.New(t.Context(), append([]graceful.Option{graceful.WithSignals()}, opts...)...)

	t.Cleanup(func() {
		notifier.Shutdown()
		waitShutdown(t, notifier)
	})

	return notifier, ctx
}

func register(t *testing.T, notifier *graceful.ShutdownNotifier, sub graceful.Subscriber) func() {
	t.Helper()

	unregister, err := notifier.Register(sub)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	return unregister
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

		register(t, notifier, kept)

		unregister := register(t, notifier, removed)

		got := notifier.SubscriberCount()
		if got != 2 {
			t.Fatalf("SubscriberCount() = %d, want 2", got)
		}

		unregister()
		unregister()

		got = notifier.SubscriberCount()
		if got != 1 {
			t.Fatalf("SubscriberCount() after unregister = %d, want 1", got)
		}

		notifier.Shutdown()
		waitShutdown(t, notifier)

		keptCalls := kept.callCount.Load()
		if keptCalls != 1 {
			t.Errorf("kept subscriber calls = %d, want 1", keptCalls)
		}

		removedCalls := removed.callCount.Load()
		if removedCalls != 0 {
			t.Errorf("removed subscriber calls = %d, want 0", removedCalls)
		}
	})
}

func TestUnregisterUncomparableSubscriber(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)

		unregister := register(t, notifier, subscriberFunc(func(context.Context) error { return nil }))
		unregister()

		got := notifier.SubscriberCount()
		if got != 0 {
			t.Errorf("SubscriberCount() after unregister = %d, want 0", got)
		}
	})
}

func TestUnregisterReleasesSubscriber(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)
		register(t, notifier, &mockSubscriber{})

		ptr := registerAndUnregister(t, notifier)

		runtime.GC()

		if ptr.Value() != nil {
			t.Error("unregister kept a reference to the removed subscriber")
		}
	})
}

func registerAndUnregister(t *testing.T, notifier *graceful.ShutdownNotifier) weak.Pointer[mockSubscriber] {
	t.Helper()

	sub := &mockSubscriber{}
	register(t, notifier, sub)()

	return weak.Make(sub)
}

func TestRegisterFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sub      graceful.Subscriber
		shutdown bool
		want     error
	}{
		{name: "nil subscriber", sub: nil, want: graceful.ErrNilSubscriber},
		{name: "after shutdown started", sub: &mockSubscriber{}, shutdown: true, want: graceful.ErrShutdownStarted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				notifier, _ := newNotifier(t)

				if tt.shutdown {
					notifier.Shutdown()
				}

				_, err := notifier.Register(tt.sub)
				if !errors.Is(err, tt.want) {
					t.Errorf("Register() error = %v, want %v", err, tt.want)
				}

				got := notifier.SubscriberCount()
				if got != 0 {
					t.Errorf("SubscriberCount() = %d, want 0", got)
				}
			})
		})
	}
}

func TestShutdownNotifiesSubscribersOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)
		sub := &mockSubscriber{}
		register(t, notifier, sub)

		for range 10 {
			go notifier.Shutdown()
		}

		waitShutdown(t, notifier)
		notifier.Shutdown()

		got := sub.callCount.Load()
		if got != 1 {
			t.Errorf("subscriber calls = %d, want 1", got)
		}
	})
}

func TestSubscribersStopInReverseOrder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := newNotifier(t)

		var order []int

		for i := range 3 {
			register(t, notifier, &mockSubscriber{onShutdownFunc: func(context.Context) error {
				order = append(order, i)

				return nil
			}})
		}

		notifier.Shutdown()
		waitShutdown(t, notifier)

		want := []int{2, 1, 0}
		if !slices.Equal(order, want) {
			t.Errorf("shutdown order = %v, want %v", order, want)
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

		register(t, notifier, &mockSubscriber{onShutdownFunc: func(context.Context) error {
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

		var (
			unregister  func()
			registerErr error
		)

		unregister = register(t, notifier, &mockSubscriber{onShutdownFunc: func(context.Context) error {
			_, registerErr = notifier.Register(&mockSubscriber{})

			unregister()

			return nil
		}})

		notifier.Shutdown()
		waitShutdown(t, notifier)

		if !errors.Is(registerErr, graceful.ErrShutdownStarted) {
			t.Errorf("Register() inside OnShutdown error = %v, want %v", registerErr, graceful.ErrShutdownStarted)
		}

		got := notifier.SubscriberCount()
		if got != 0 {
			t.Errorf("SubscriberCount() = %d, want 0", got)
		}
	})
}

func TestWaitReturnsSubscriberErrors(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, _ := graceful.New(t.Context(), graceful.WithSignals())
		succeeding := &mockSubscriber{}
		failing := &mockSubscriber{onShutdownFunc: func(context.Context) error { return errShutdownFailed }}

		register(t, notifier, succeeding)
		register(t, notifier, failing)

		notifier.Shutdown()

		err := notifier.Wait()
		if !errors.Is(err, errShutdownFailed) {
			t.Errorf("Wait() error = %v, want %v", err, errShutdownFailed)
		}

		got := succeeding.callCount.Load()
		if got != 1 {
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

				register(t, notifier, &mockSubscriber{onShutdownFunc: func(ctx context.Context) error {
					secondErr = ctx.Err()

					return nil
				}})
				register(t, notifier, &mockSubscriber{onShutdownFunc: func(ctx context.Context) error {
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Error("OnShutdown ctx has no deadline")
					}

					gotTimeout = deadline.Sub(start)

					<-ctx.Done()

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

func TestNonPositiveTimeoutMeansNoDeadline(t *testing.T) {
	t.Parallel()

	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				notifier, _ := newNotifier(t, graceful.WithTimeout(timeout))

				var (
					hasDeadline bool
					ctxErr      error
				)

				register(t, notifier, &mockSubscriber{onShutdownFunc: func(ctx context.Context) error {
					_, hasDeadline = ctx.Deadline()
					ctxErr = ctx.Err()

					return nil
				}})

				notifier.Shutdown()
				waitShutdown(t, notifier)

				if hasDeadline {
					t.Error("OnShutdown ctx has a deadline, want none")
				}

				if ctxErr != nil {
					t.Errorf("OnShutdown ctx.Err() = %v, want nil", ctxErr)
				}
			})
		})
	}
}

func TestSubscriberContextKeepsParentValues(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const want = "value"

		parent := context.WithValue(t.Context(), ctxKey{}, want)
		notifier, _ := graceful.New(parent, graceful.WithSignals())

		var got any

		register(t, notifier, &mockSubscriber{onShutdownFunc: func(ctx context.Context) error {
			got = ctx.Value(ctxKey{})

			return nil
		}})

		notifier.Shutdown()
		waitShutdown(t, notifier)

		if got != want {
			t.Errorf("ctx.Value() inside OnShutdown = %v, want %q", got, want)
		}
	})
}

func TestShutdownCause(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		notifier, ctx := newNotifier(t)

		notifier.Shutdown()
		<-ctx.Done()

		got := context.Cause(ctx)
		if !errors.Is(got, graceful.ErrShutdownRequested) {
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
		register(t, notifier, sub)

		cancel(errParentStopped)
		waitShutdown(t, notifier)

		calls := sub.callCount.Load()
		if calls != 1 {
			t.Errorf("subscriber calls = %d, want 1", calls)
		}

		cause := context.Cause(ctx)
		if !errors.Is(cause, errParentStopped) {
			t.Errorf("context.Cause() = %v, want %v", cause, errParentStopped)
		}
	})
}

func TestSignalErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  graceful.SignalError
		want string
	}{
		{name: "with signal", err: graceful.SignalError{Signal: os.Interrupt}, want: "received signal interrupt"},
		{name: "without signal", err: graceful.SignalError{Signal: nil}, want: "received signal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.err.Error()
			if got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}
