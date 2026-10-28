package graceful

import (
	"errors"
	"os"
)

var ErrShutdownRequested = errors.New("shutdown requested")

// SignalError is the context cause when shutdown is started by a signal.
type SignalError struct {
	Signal os.Signal
}

func (e SignalError) Error() string {
	return "received signal " + e.Signal.String()
}
