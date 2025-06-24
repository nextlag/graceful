# Graceful Shutdown

Пакет для корректного завершения работы Go-приложений с поддержкой graceful shutdown.

## Установка

```bash
go get github.com/nextlag/graceful
```

## Использование

### Базовый пример

```go
package main

import (
    "context"
    "log/slog"
    "net/http"
    
    "github.com/nextlag/graceful"
)

func main() {
    // Создаём менеджер graceful shutdown
    shutdown := graceful.New(slog.Default())
    
    // Запускаем HTTP сервер
    server := &http.Server{Addr: ":8080"}
    
    // Регистрируем сервер как подписчика на shutdown
    shutdown.Register(&httpServerSubscriber{server: server})
    
    // Запускаем сервер
    go func() {
        if err := server.ListenAndServe(); err != http.ErrServerClosed {
            slog.Error("server error", "error", err)
        }
    }()
    
    // Ожидаем сигнал завершения
    shutdown.Wait()
}

// Реализация интерфейса Subscriber для HTTP сервера
type httpServerSubscriber struct {
    server *http.Server
}

func (h *httpServerSubscriber) OnShutdown(ctx context.Context) error {
    return h.server.Shutdown(ctx)
}
```

### Работа с несколькими сервисами

```go
func main() {
    shutdown := graceful.New(nil) // используется slog.Default()
    
    // Регистрируем несколько сервисов
    shutdown.Register(&databaseService{})
    shutdown.Register(&cacheService{})
    shutdown.Register(&httpService{})
    
    // Запускаем сервисы...
    
    // Ожидаем завершения
    shutdown.Wait()
}
```

### Использование контекста

```go
func main() {
    shutdown := graceful.New(nil)
    
    // Получаем контекст, который будет отменён при shutdown
    ctx := shutdown.Context()
    
    // Используем контекст в горутинах
    go func() {
        ticker := time.NewTicker(time.Second)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                // выполняем периодическую работу
            case <-ctx.Done():
                // контекст отменён, завершаем работу
                return
            }
        }
    }()
    
    shutdown.Wait()
}
```

### Программное завершение

```go
func main() {
    shutdown := graceful.New(nil)
    
    // Завершение программно через 10 секунд
    go func() {
        time.Sleep(10 * time.Second)
        shutdown.Shutdown()
    }()
    
    shutdown.Wait()
}
```

## API

### Создание

```go
func New(log *slog.Logger) *ShutdownNotifier
```

Создаёт новый экземпляр ShutdownNotifier. Если logger равен nil, используется `slog.Default()`.

### Методы

#### Register

```go
func (sn *ShutdownNotifier) Register(sub Subscriber)
```

Регистрирует подписчика, который будет уведомлён при завершении работы.

#### Unregister

```go
func (sn *ShutdownNotifier) Unregister(sub Subscriber)
```

Удаляет подписчика из списка уведомлений.

#### Wait

```go
func (sn *ShutdownNotifier) Wait()
```

Блокирует выполнение до получения сигнала завершения.

#### Shutdown

```go
func (sn *ShutdownNotifier) Shutdown()
```

Инициирует процесс завершения работы программно.

#### Context

```go
func (sn *ShutdownNotifier) Context() context.Context
```

Возвращает контекст, который будет отменён при завершении работы.

#### Done

```go
func (sn *ShutdownNotifier) Done() <-chan struct{}
```

Возвращает канал, который будет закрыт при завершении работы.

#### IsShuttingDown

```go
func (sn *ShutdownNotifier) IsShuttingDown() bool
```

Проверяет, находится ли приложение в процессе завершения.

#### SubscriberCount

```go
func (sn *ShutdownNotifier) SubscriberCount() int
```

Возвращает количество зарегистрированных подписчиков.

## Интерфейс Subscriber

```go
type Subscriber interface {
    OnShutdown(ctx context.Context) error
}
```

Любой тип, реализующий этот интерфейс, может быть зарегистрирован как подписчик. Метод `OnShutdown` будет вызван при завершении работы с контекстом, имеющим таймаут 30 секунд.

## Обрабатываемые сигналы

- `SIGHUP` - обычно используется для перезагрузки конфигурации
- `SIGINT` - прерывание от клавиатуры (Ctrl+C)
- `SIGTERM` - запрос на завершение
- `SIGUSR1` - пользовательский сигнал (используется для программного shutdown)

## Особенности

- Все подписчики уведомляются последовательно
- Каждый подписчик имеет 30 секунд на завершение работы
- Если подписчик возвращает ошибку, она логируется, но не прерывает процесс уведомления других подписчиков
- Метод `Shutdown()` можно вызывать многократно - он выполнится только один раз
- При каждом вызове `Context()` создаётся новый контекст с отдельной горутиной

## Примеры использования

### Graceful shutdown для gRPC сервера

```go
type grpcServerSubscriber struct {
    server *grpc.Server
}

func (g *grpcServerSubscriber) OnShutdown(ctx context.Context) error {
    stopped := make(chan struct{})
    go func() {
        g.server.GracefulStop()
        close(stopped)
    }()
    
    select {
    case <-stopped:
        return nil
    case <-ctx.Done():
        g.server.Stop()
        return ctx.Err()
    }
}
```

### Сохранение состояния при завершении

```go
type stateManager struct {
    mu    sync.Mutex
    state map[string]interface{}
}

func (s *stateManager) OnShutdown(ctx context.Context) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    data, err := json.Marshal(s.state)
    if err != nil {
        return err
    }
    
    return os.WriteFile("state.json", data, 0644)
}
```