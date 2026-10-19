# Reading this Go code

## Start at main

`cmd/queue-service/main.go` declares `package main` and defines `func main()`, the executable's entry point. All Go files in this directory compile as one package. `main.go`, `server.go`, `client.go` and `inspect.go` therefore form one executable.

`main` creates a JSON logger, obtains a context canceled by SIGINT or SIGTERM, and calls `run`. Deferred calls close resources before returning to `main`. Only `main` calls `os.Exit`, because an early exit would skip deferred cleanup.

| Command | Work performed |
| --- | --- |
| `serve` | Connect to RabbitMQ, declare queues, listen for gRPC |
| `worker` | Connect to RabbitMQ and PostgreSQL, process deliveries |
| `publish` | Send one gRPC publication |
| `health` | Call standard gRPC health checking |
| `inspect` | Query source and dead-letter queue counts |
| `peek-dead` | Receive and requeue one dead letter |

## Packages and visibility

`internal/message`, `internal/broker`, `internal/server`, `internal/worker` and `internal/receipt` are separate packages. Go restricts importing these `internal` packages from outside the repository's parent tree. The generated contract is outside `internal`, so another Go module can import its client.

Names starting with uppercase letters are exported: `Message`, `Publish`, `Record`. Lowercase names stay within their package: `wait`, `decode`, `serve`. There are no `public` or `private` declaration keywords.

## Structs and methods

`message.Message` is a struct containing the envelope. `func (m Message) Validate() error` attaches a method to it. `(m Message)` is the receiver. This value receiver does not mutate the original envelope.

`func (s *Store) Record(...)` uses a pointer receiver and works with the existing store and its shared connection pool. Short receivers such as `s`, `w` and `p` are common Go conventions. Other names describe messages, queues and results rather than encoding their Go types.

## Interfaces describe required behavior

The gRPC adapter needs a publisher, so it declares a small `Publisher` interface. `broker.Publisher` satisfies it by having a matching `Publish` method. There is no explicit `implements` declaration.

The consumer similarly declares `Recorder` with one `Record` method. The receipt store satisfies it; tests substitute a function adapter. Each interface belongs to its consumer and contains only the behavior needed there.

## Errors are return values

Operations commonly return `(result, error)`. A non-nil error is handled immediately. `fmt.Errorf("context: %w", err)` adds information while retaining the underlying cause. `errors.Is` recognizes a cause through wrapped errors.

The gRPC adapter converts internal failures into status codes. Connection URLs contain credentials, so they are not logged or returned to clients.

## Context supplies cancellation and deadlines

`context.Context` is the first parameter of operations that can wait. gRPC supplies the request context. `context.WithTimeout` bounds broker operations; an earlier caller deadline still wins.

`WithTimeout` and `WithCancel` return cancellation functions that release resources and stop dependent work. Database operations honor context. Retry delays select between a timer and cancellation. Socket write deadlines additionally bound AMQP writes because the library's publish context alone does not interrupt every network operation.

## Goroutines and channels

`go function()` starts a goroutine. Goroutines serve gRPC, consume messages and coordinate shutdown. `sync.WaitGroup` joins consumer goroutines before their database and broker resources are closed.

A Go channel passes values between goroutines. The publisher's buffered `slots` channel acts as a semaphore: sending reserves capacity, receiving releases it. `select` waits for either capacity or cancellation.

The AMQP client supplies the delivery channel. If it closes unexpectedly, the worker exits instead of remaining alive without receiving work. Docker restarts the process. Connection recovery is explicit at the process boundary.

## Tests to read next

- `internal/server/server_test.go`: validation, status mapping and gRPC serialization.
- `internal/worker/worker_test.go`: acknowledgment decisions and cancellation after commit.
- `integration/queue_test.go`: real dependencies and a killed consumer inside the commit-to-ACK window.

The generated `.pb.go` files implement protocol machinery. Read the `.proto` definition first; the handwritten request and delivery paths can be understood independently of generated internals.
