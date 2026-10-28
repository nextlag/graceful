package graceful

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

// Subscriber is an interface for subscribers responding to the completion of work.
type Subscriber interface {
	OnShutdown(ctx context.Context) error
}

type options struct {
	signals    []os.Signal
	signalsSet bool
	timeout    time.Duration
}

// Option configures a ShutdownNotifier.
type Option func(options) options

// WithSignals sets the signals that start shutdown; with no signals shutdown starts only via Shutdown or ctx.
func WithSignals(signals ...os.Signal) Option {
	return func(o options) options {
		o.signals, o.signalsSet = signals, true

		return o
	}
}

// WithTimeout sets the total time all subscribers have to shut down.
func WithTimeout(timeout time.Duration) Option {
	return func(o options) options {
		o.timeout = timeout

		return o
	}
}

// ShutdownNotifier controls subscribers and processes signals.
type ShutdownNotifier struct {
	mx             sync.Mutex
	subscriberList []Subscriber
	timeout        time.Duration
	cancel         context.CancelCauseFunc
	stopping       <-chan struct{}
	done           chan struct{}
	err            error
}

// New creates a ShutdownNotifier and returns a context that's cancelled when shutdown starts.
func New(ctx context.Context, opts ...Option) (*ShutdownNotifier, context.Context) {
	const shutdownTimeout = 30 * time.Second

	cfg := options{signals: nil, signalsSet: false, timeout: shutdownTimeout}
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	defaultSignals := [...]os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1}
	if !cfg.signalsSet {
		cfg.signals = defaultSignals[:]
	}

	ctx, cancel := context.WithCancelCause(ctx)

	notifier := &ShutdownNotifier{
		mx:             sync.Mutex{},
		subscriberList: nil,
		timeout:        cfg.timeout,
		cancel:         cancel,
		stopping:       ctx.Done(),
		done:           make(chan struct{}),
		err:            nil,
	}

	sig := make(chan os.Signal, 1)

	for _, s := range cfg.signals {
		if !signal.Ignored(s) {
			signal.Notify(sig, s)
		}
	}

	go notifier.run(sig)

	return notifier, ctx
}

// Register adds a subscriber to be notified on shutdown.
func (sn *ShutdownNotifier) Register(sub Subscriber) {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	sn.subscriberList = append(sn.subscriberList, sub)
}

// Unregister removes a subscriber from the notification list.
func (sn *ShutdownNotifier) Unregister(sub Subscriber) {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	for i, s := range sn.subscriberList {
		if s == sub {
			sn.subscriberList = append(sn.subscriberList[:i], sn.subscriberList[i+1:]...)

			return
		}
	}
}

// SubscriberCount returns the number of registered subscribers.
func (sn *ShutdownNotifier) SubscriberCount() int {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	return len(sn.subscriberList)
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

func (sn *ShutdownNotifier) run(sig chan os.Signal) {
	select {
	case received := <-sig:
		sn.cancel(SignalError{Signal: received})
	case <-sn.stopping:
	}

	signal.Stop(sig)

	sn.err = sn.notifySubscribers()

	close(sn.done)
}

func (sn *ShutdownNotifier) notifySubscribers() error {
	ctx, cancel := context.WithTimeout(context.Background(), sn.timeout)
	defer cancel()

	var errs []error

	for _, sub := range sn.subscribersSnapshot() {
		err := sub.OnShutdown(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (sn *ShutdownNotifier) subscribersSnapshot() []Subscriber {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	return slices.Clone(sn.subscriberList)
}
