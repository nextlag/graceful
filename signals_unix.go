//go:build unix

package graceful

import (
	"os"
	"syscall"
)

func defaultSignals() []os.Signal {
	return []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1}
}
