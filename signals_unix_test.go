//go:build unix

package graceful_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"testing"

	"github.com/nextlag/graceful"
)

func TestDefaultSignalsStartShutdown(t *testing.T) {
	t.Parallel()

	for _, sig := range []syscall.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()

			if !runInChild(t) {
				return
			}

			notifier, ctx := graceful.New(t.Context())
			sub := &mockSubscriber{}
			register(t, notifier, sub)

			err := syscall.Kill(syscall.Getpid(), sig)
			if err != nil {
				t.Fatalf("Kill() error = %v", err)
			}

			waitShutdown(t, notifier)

			calls := sub.callCount.Load()
			if calls != 1 {
				t.Errorf("subscriber calls = %d, want 1", calls)
			}

			var signalErr graceful.SignalError

			if !errors.As(context.Cause(ctx), &signalErr) || signalErr.Signal != sig {
				t.Errorf("context.Cause() = %v, want SignalError{%v}", context.Cause(ctx), sig)
			}
		})
	}
}

func TestIgnoredSignalStaysIgnored(t *testing.T) {
	t.Parallel()

	if !runInChild(t) {
		return
	}

	signal.Ignore(syscall.SIGHUP)

	notifier, _ := graceful.New(t.Context(), graceful.WithSignals(syscall.SIGHUP))

	if !signal.Ignored(syscall.SIGHUP) {
		t.Error("New() stopped ignoring SIGHUP, want it to stay ignored")
	}

	notifier.Shutdown()
	waitShutdown(t, notifier)
}

func runInChild(t *testing.T) bool {
	t.Helper()

	const childEnv = "GRACEFUL_CHILD_TEST"

	if os.Getenv(childEnv) == t.Name() {
		return true
	}

	pattern := strings.Split(t.Name(), "/")
	for i, name := range pattern {
		pattern[i] = "^" + regexp.QuoteMeta(name) + "$"
	}

	args := []string{"-test.run=" + strings.Join(pattern, "/")}

	coverDir := flag.Lookup("test.gocoverdir")
	if coverDir != nil && coverDir.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+coverDir.Value.String())
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], args...)

	cmd.Env = append(os.Environ(), childEnv+"="+t.Name())

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("child process error = %v\n%s", err, out)
	}

	return false
}
