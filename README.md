# graceful

Graceful shutdown для Go-приложений.

```bash
go get github.com/nextlag/graceful@v1.0.1
```

Go 1.25+.

## Пример

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/nextlag/graceful"
)

type httpServer struct {
	*http.Server
}

func (s httpServer) OnShutdown(ctx context.Context) error {
	return s.Shutdown(ctx)
}

func main() {
	err := run()
	if err != nil {
		slog.Error("app failed", slog.Any("err", err))
		os.Exit(1)
	}
}

func run() error {
	shutdown, ctx := graceful.New(context.Background(), graceful.WithTimeout(10*time.Second))

	server := &http.Server{
		Addr:        ":8080",
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	shutdown.Register(httpServer{server})

	go func() {
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", slog.Any("err", err))
			shutdown.Shutdown()
		}
	}()

	return shutdown.Wait()
}
```

## Поведение

- Остановку запускает первое из: сигнал, `Shutdown()`, отмена родительского `ctx`.
- Сигналы по умолчанию: `SIGHUP`, `SIGINT`, `SIGTERM`, `SIGUSR1`. Игнорируемые процессом (`nohup`) не перехватываются.
- Контекст из `New` отменяется в начале остановки. `context.Cause(ctx)`: `SignalError`, `ErrShutdownRequested` или причина родителя.
- Подписчики вызываются по очереди в порядке регистрации, с общим таймаутом (30 с).
- `Wait()` возвращает ошибки подписчиков через `errors.Join`.

## API

```go
func New(ctx context.Context, opts ...Option) (*ShutdownNotifier, context.Context)

func WithSignals(signals ...os.Signal) Option 
func WithTimeout(timeout time.Duration) Option

func (sn *ShutdownNotifier) Register(sub Subscriber)
func (sn *ShutdownNotifier) Unregister(sub Subscriber)
func (sn *ShutdownNotifier) SubscriberCount() int
func (sn *ShutdownNotifier) Shutdown()
func (sn *ShutdownNotifier) IsShuttingDown() bool
func (sn *ShutdownNotifier) Done() <-chan struct{}
func (sn *ShutdownNotifier) Wait() error

type Subscriber interface {
	OnShutdown(ctx context.Context) error
}

var ErrShutdownRequested = errors.New("shutdown requested")

type SignalError struct {
	Signal os.Signal
}
```