package graceful

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"slices"
	"sync"
	"time"
)

// Subscriber is stopped by ShutdownNotifier when shutdown starts.
type Subscriber interface {
	// OnShutdown stops the subscriber within ctx; it must not call Wait or wait for Done.
	OnShutdown(ctx context.Context) error
}

type options struct {
	signals []os.Signal
	timeout time.Duration
}

// Option configures a ShutdownNotifier.
type Option func(options) options

// WithSignals sets the signals that start shutdown; with no signals shutdown starts only via Shutdown or ctx.
func WithSignals(signals ...os.Signal) Option {
	return func(o options) options {
		o.signals = signals

		return o
	}
}

// WithTimeout sets the total time all subscribers have to shut down; zero or negative means no timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(o options) options {
		o.timeout = timeout

		return o
	}
}

type subscription struct {
	sub Subscriber
}

// ShutdownNotifier controls subscribers and processes signals.
type ShutdownNotifier struct {
	mx            sync.Mutex
	subscriptions []*subscription
	timeout       time.Duration
	cancel        context.CancelCauseFunc
	stopping      <-chan struct{}
	done          chan struct{}
	err           error
}

// New creates a ShutdownNotifier and returns a context that's cancelled when shutdown starts.
func New(ctx context.Context, opts ...Option) (*ShutdownNotifier, context.Context) {
	const shutdownTimeout = 30 * time.Second

	cfg := options{signals: defaultSignals(), timeout: shutdownTimeout}
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	ctx, cancel := context.WithCancelCause(ctx)

	notifier := &ShutdownNotifier{
		mx:            sync.Mutex{},
		subscriptions: nil,
		timeout:       cfg.timeout,
		cancel:        cancel,
		stopping:      ctx.Done(),
		done:          make(chan struct{}),
		err:           nil,
	}

	sig := make(chan os.Signal, 1)

	for _, s := range cfg.signals {
		if !signal.Ignored(s) {
			signal.Notify(sig, s)
		}
	}

	go notifier.run(ctx, sig)

	return notifier, ctx
}

// Register adds a subscriber and returns a function that removes it.
func (sn *ShutdownNotifier) Register(sub Subscriber) (func(), error) {
	if sub == nil {
		return nil, ErrNilSubscriber
	}

	sn.mx.Lock()
	defer sn.mx.Unlock()

	if sn.IsShuttingDown() {
		return nil, ErrShutdownStarted
	}

	s := &subscription{sub: sub}

	sn.subscriptions = append(sn.subscriptions, s)

	return func() { sn.unregister(s) }, nil
}

// SubscriberCount returns the number of registered subscribers.
func (sn *ShutdownNotifier) SubscriberCount() int {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	return len(sn.subscriptions)
}

// Shutdown initiates the shutdown process.
func (sn *ShutdownNotifier) Shutdown() {
	sn.cancel(ErrShutdownRequested)
}

// IsShuttingDown returns true if shutdown has been initiated.
func (sn *ShutdownNotifier) IsShuttingDown() bool {
	select {
	case <-sn.stopping:
		return true
	default:
		return false
	}
}

// Done returns a channel that's closed when shutdown is complete.
func (sn *ShutdownNotifier) Done() <-chan struct{} {
	return sn.done
}

// Wait blocks until shutdown is complete and returns the subscribers' errors.
func (sn *ShutdownNotifier) Wait() error {
	<-sn.done

	return sn.err
}

func (sn *ShutdownNotifier) unregister(s *subscription) {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	i := slices.Index(sn.subscriptions, s)
	if i >= 0 {
		sn.subscriptions = slices.Delete(sn.subscriptions, i, i+1)
	}
}

func (sn *ShutdownNotifier) run(ctx context.Context, sig chan os.Signal) {
	select {
	case received := <-sig:
		sn.cancel(SignalError{Signal: received})
		signal.Stop(sig)

	case <-ctx.Done():
		go sn.stopOnSignal(sig)
	}

	sn.err = sn.notifySubscribers(ctx)

	close(sn.done)
}

func (sn *ShutdownNotifier) stopOnSignal(sig chan os.Signal) {
	select {
	case <-sig:
	case <-sn.done:
	}

	signal.Stop(sig)
}

func (sn *ShutdownNotifier) notifySubscribers(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)

	if sn.timeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, sn.timeout)
		defer cancel()
	}

	var errs []error

	for _, s := range slices.Backward(sn.subscriptionsSnapshot()) {
		err := s.sub.OnShutdown(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (sn *ShutdownNotifier) subscriptionsSnapshot() []*subscription {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	return slices.Clone(sn.subscriptions)
}
