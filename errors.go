package graceful

import (
	"errors"
	"os"
)

var (
	// ErrShutdownRequested is the context cause when shutdown is started by Shutdown.
	ErrShutdownRequested = errors.New("shutdown requested")
	// ErrShutdownStarted is returned by Register once shutdown has started.
	ErrShutdownStarted = errors.New("shutdown already started")
	// ErrNilSubscriber is returned by Register for a nil subscriber.
	ErrNilSubscriber = errors.New("nil subscriber")
)

// SignalError is the context cause when shutdown is started by a signal.
type SignalError struct {
	Signal os.Signal
}

func (e SignalError) Error() string {
	const msg = "received signal"

	if e.Signal == nil {
		return msg
	}

	return msg + " " + e.Signal.String()
}
