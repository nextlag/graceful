# graceful

Graceful shutdown для Go-приложений.

```bash
go get github.com/nextlag/graceful@v1.1.0
```

Go 1.25+.

## Пример

```go
package main

import (
	"context"
	"errors"
	"log/slog"
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
	shutdown, _ := graceful.New(context.Background(), graceful.WithTimeout(10*time.Second))

	server := &http.Server{Addr: ":8080", ReadHeaderTimeout: 5 * time.Second}

	_, err := shutdown.Register(httpServer{server})
	if err != nil {
		return err
	}

	serveErr := make(chan error, 1)

	go func() {
		defer close(serveErr)

		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			shutdown.Shutdown()
		}
	}()

	return errors.Join(shutdown.Wait(), <-serveErr)
}
```

## Поведение

- Остановку запускает первое из: сигнал, `Shutdown()`, отмена родительского `ctx`.
- Сигналы по умолчанию: unix — `SIGHUP`, `SIGINT`, `SIGTERM`, `SIGUSR1`; остальные ОС — `os.Interrupt`, `SIGTERM`. Игнорируемые процессом (`nohup`) не перехватываются.
- Повторный сигнал во время остановки завершает процесс. Если остановка запущена не сигналом, первый сигнал во время неё поглощается.
- Контекст из `New` отменяется в начале остановки. `context.Cause(ctx)`: `SignalError`, `ErrShutdownRequested` или причина родителя.
- Подписчики вызываются по очереди в обратном порядке регистрации, с общим таймаутом (30 с). Таймаут ≤ 0 — без ограничения. Контекст подписчика сохраняет значения родительского `ctx`, но не его отмену.
- `Register` возвращает функцию отписки. После начала остановки `Register` возвращает `ErrShutdownStarted`, а отписка уже не отменяет вызов подписчика. Для `nil` — `ErrNilSubscriber`.
- `Wait()` возвращает ошибки подписчиков через `errors.Join`. Из `OnShutdown` вызывать `Wait()` и ждать `Done()` нельзя - дедлок.

## API

```go
func New(ctx context.Context, opts ...Option) (*ShutdownNotifier, context.Context)

func WithSignals(signals ...os.Signal) Option
func WithTimeout(timeout time.Duration) Option

func (sn *ShutdownNotifier) Register(sub Subscriber) (func(), error)
func (sn *ShutdownNotifier) SubscriberCount() int
func (sn *ShutdownNotifier) Shutdown()
func (sn *ShutdownNotifier) IsShuttingDown() bool
func (sn *ShutdownNotifier) Done() <-chan struct{}
func (sn *ShutdownNotifier) Wait() error

type Subscriber interface {
	OnShutdown(ctx context.Context) error
}

var (
	ErrShutdownRequested = errors.New("shutdown requested")
	ErrShutdownStarted   = errors.New("shutdown already started")
	ErrNilSubscriber     = errors.New("nil subscriber")
)

type SignalError struct {
	Signal os.Signal
}
```