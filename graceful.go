package graceful

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Subscriber is an interface for subscribers responding to the completion of work.
type Subscriber interface {
	OnShutdown(ctx context.Context) error
}

// ShutdownNotifier controls subscribers and processes signals.
type ShutdownNotifier struct {
	mx             sync.Mutex
	subscriberList []Subscriber
	sig            chan os.Signal
	done           chan struct{}
	log            *slog.Logger
	once           sync.Once
}

// New creates a new ShutdownNotifier instance.
func New(log *slog.Logger) *ShutdownNotifier {
	if log == nil {
		log = slog.Default()
	}

	notifier := &ShutdownNotifier{
		subscriberList: make([]Subscriber, 0),
		sig:            make(chan os.Signal, 1),
		done:           make(chan struct{}),
		log:            log,
	}

	signal.Notify(notifier.sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		sig := <-notifier.sig

		notifier.log.Info("signal has been received, shutting down", slog.String("signal", sig.String()))

		signal.Stop(notifier.sig)

		notifier.notifySubscribers()

		close(notifier.done)
	}()

	return notifier
}

// Register adds a subscriber to be notified on shutdown.
func (sn *ShutdownNotifier) Register(sub Subscriber) {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	sn.subscriberList = append(sn.subscriberList, sub)
}

// Wait blocks until shutdown is complete.
func (sn *ShutdownNotifier) Wait() {
	<-sn.done
}

// Shutdown initiates the shutdown process.
func (sn *ShutdownNotifier) Shutdown() {
	sn.once.Do(func() {
		sn.sig <- syscall.SIGUSR1
	})
}

// Done returns a channel that's closed when shutdown is complete.
func (sn *ShutdownNotifier) Done() <-chan struct{} {
	return sn.done
}

// Context returns a context that's cancelled when shutdown is complete.
func (sn *ShutdownNotifier) Context() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-sn.done
		cancel()
	}()

	return ctx
}

// IsShuttingDown returns true if shutdown has been initiated.
func (sn *ShutdownNotifier) IsShuttingDown() bool {
	select {
	case <-sn.done:
		return true
	default:
		return false
	}
}

// SubscriberCount returns the number of registered subscribers.
func (sn *ShutdownNotifier) SubscriberCount() int {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	return len(sn.subscriberList)
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

const shutdownTimeout = 30 * time.Second

func (sn *ShutdownNotifier) notifySubscribers() {
	sn.mx.Lock()
	defer sn.mx.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	for _, sub := range sn.subscriberList {
		if err := sub.OnShutdown(ctx); err != nil {
			sn.log.Error("shutdown failed", slog.String("error", err.Error()))
		}
	}
}
