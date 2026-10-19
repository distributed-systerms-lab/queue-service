# Queue Service

A Go service for publishing messages over gRPC and consuming them through RabbitMQ. Part of the Distributed Systems Lab.

Includes durable quorum queues, publisher confirmations, manual ACK/NACK, bounded retries, dead-letter queues, concurrent consumers and a durable reference handler that suppresses duplicate effects.

## Start the lab

Prerequisites: Docker Engine and Docker Compose v2. Go is only needed for local development.

```sh
git clone https://github.com/distributed-systerms-lab/queue-service.git
cd queue-service
cp .env.example .env
```

Set both passwords in `.env`. Use URI-safe passwords, for example the output of `openssl rand -hex 24`.

Set `LAB_NETWORK` to your existing shared lab network. Compose references it as external and never creates or deletes it. If it does not exist yet, choose an unused subnet according to your lab address plan and create the network separately:

```sh
# Example: first verify this subnet does not overlap your LAN, VPN or Docker networks.
docker network create --driver bridge --subnet 172.30.10.0/24 lab-messaging
```

```sh
docker compose up -d --build --wait
docker compose exec queue-service /queue-service publish \
  -id first-message -type lab.echo -payload 'hello from the lab' \
  -correlation-id first-run
docker compose logs -f consumer
```

`message accepted` means RabbitMQ confirmed publication. `message processed` means PostgreSQL committed a receipt. `message acknowledged` means the consumer sent ACK. Read the [delivery contract](docs/delivery-contract.md) for the gaps between these events.

No ports are published on the host. Containers on `LAB_NETWORK` reach the API at `queue-service:50051`. The included CLI runs inside a container until the Test Harness is available.

## Read the code

Start with [the Go reading guide](docs/go-reading-guide.md), then follow:

1. [`api/queue/v1/queue.proto`](api/queue/v1/queue.proto): public contract.
2. [`internal/message/message.go`](internal/message/message.go): envelope and validation.
3. [`internal/server/server.go`](internal/server/server.go): request handling.
4. [`internal/broker/publisher.go`](internal/broker/publisher.go): routing and broker confirmation.
5. [`internal/worker/worker.go`](internal/worker/worker.go): processing, retry and acknowledgment.
6. [`internal/receipt/store.go`](internal/receipt/store.go): durable duplicate suppression.
7. [`cmd/queue-service`](cmd/queue-service): configuration, process lifetime and CLI.

`gen/` contains generated code. Read the `.proto` first; generated files are committed so ordinary builds require no generator.

## Inspect and experiment

```sh
docker compose exec queue-service /queue-service inspect -queue tasks
docker compose exec queue-service /queue-service publish -id poison-1 -type lab.fail
docker compose exec queue-service /queue-service peek-dead -queue tasks
```

`inspect` reports ready messages and consumers. `peek-dead` prints one dead letter and requeues it; the JSON payload is base64-encoded. See [failure experiments](docs/failure-experiments.md) for retries, duplicates, process death, persistence and scaling.

## Develop and verify

Use the Go version declared in `go.mod`. Dependencies are pinned in `go.mod` and `go.sum`.

```sh
make check                  # Formatting, go vet and tests with the race detector.
make audit                  # Scan reachable code for known Go vulnerabilities.
make build                  # bin/queue-service
make integration-up         # Disposable RabbitMQ and PostgreSQL containers.
make integration            # Real broker, database, gRPC and process-death tests.
make integration-down       # Remove disposable test containers and their data.
```

The test-only Compose binds dependency ports 5672 and 5432 to loopback. These ports must be free. The [CI workflow](.github/workflows/ci.yaml) is configured to run the checks and build the runtime image. See the [verification record](docs/verification.md) for checks actually executed while preparing this version.

To regenerate the contract, install `protoc` 33.5, run `make tools`, then `make generate`.

## Scope

The reference handler's effect is one durable receipt containing the message envelope. PostgreSQL exists for this example consumer; the publishing API depends only on RabbitMQ. Arbitrary external business operations need their own transactional or idempotency design.

The initial broker is one node. Its durable queues survive restart when the volume survives; this is not a highly available deployment. There is no global ordering or exactly-once delivery promise.

This internal local lab uses plaintext connections, enables gRPC reflection and has no API authentication. Keep it on lab networks. [Docker operation](docs/docker-operation.md) explains network membership, volumes, restart behavior and configuration.
