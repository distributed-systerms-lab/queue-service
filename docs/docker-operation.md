# Docker operation

## How the containers reach each other

The API joins two networks: the external network named by `LAB_NETWORK`, and a Compose-managed private network. Consumers, RabbitMQ and PostgreSQL join only the private network. Other lab applications can resolve `queue-service` on the external network but cannot directly resolve or reach its private dependencies through that network.

Docker's embedded DNS resolves `rabbitmq` and `postgres` inside the private network. Connection URLs use those names; the code contains no static container addresses. `private.internal: true` restricts external connectivity on the private network.

The shared network has its own lifecycle. Create it separately according to the lab address plan. `docker compose down` removes the private network but leaves the external network intact.

```sh
docker network inspect lab-messaging
docker compose ps
docker inspect "$(docker compose ps -q queue-service)" --format '{{json .NetworkSettings.Networks}}'
```

There is no `ports` section in the application Compose. Processes still listen on their container ports; containers on an appropriate network connect directly. `expose` is not required to permit that traffic and would not provide a firewall.

## Persistent data

`rabbitmq-data` retains broker state. The broker has a stable hostname because its data directory is associated with its RabbitMQ node identity. `postgres-data` retains receipt records.

```sh
docker compose down       # Remove containers; keep named volumes.
docker compose up -d --wait
```

`docker compose down -v` also deletes both volumes and therefore both queued messages and duplicate-protection receipts. Use it only when intentionally resetting the lab.

Changing the initial database or broker password in `.env` does not rotate credentials already stored in an existing data volume. Change them through the relevant server or reset intentionally.

## Startup and recovery

Health checks gate initial startup on dependencies. They do not restart running containers merely because they become unhealthy. The API exits if its broker connection closes; consumer workers exit if their delivery stream closes. `restart: unless-stopped` then starts a fresh process with new connections.

The consumer waits for PostgreSQL at initial startup. During an outage, database failures follow the bounded retry path. `depends_on` does not repair an unavailable database or guarantee its availability after startup.

On SIGTERM, the API stops accepting calls and gives existing RPCs up to `SHUTDOWN_TIMEOUT` to finish before forceful shutdown. Consumers cancel current work and close their channels; unacknowledged messages become eligible for redelivery. This deliberately favors safe redelivery over waiting for slow handlers to drain.

The binary uses a non-root, read-only runtime container. The application needs no writable filesystem. PostgreSQL and RabbitMQ write only to their own data volumes. Application logs go to stderr; Docker's `local` logging driver rotates them with a bounded file count.

## Configuration

| Variable | Default | Effect |
| --- | --- | --- |
| `AMQP_URL` | Required by server/worker/inspection | Broker connection URI; Compose builds it from lab credentials |
| `DATABASE_URL` | Required by worker | Shared PostgreSQL receipt store |
| `GRPC_ADDRESS` | `:50051` | API listening address |
| `LAB_NETWORK` | `lab-messaging` | Existing external Docker network |
| `QUEUES` | `tasks` | Comma-separated queue allowlist, also declared at startup |
| `QUEUE_NAME` | `tasks` | Queue consumed by this process; must belong to `QUEUES` |
| `CONCURRENCY` | `4` | Consumer channels per process, each with prefetch one |
| `MAX_RETRIES` | `3` | Additional attempts; crashes also consume the broker failure budget |
| `RETRY_DELAY` | `1s` | Fixed delay before requeueing a failed delivery |
| `MAX_QUEUE_LENGTH` | `100000` | Ready-message limit per source queue; overflow rejects publication |
| `PUBLISH_CONCURRENCY` | `32` | Maximum in-flight publishing channels per API process |
| `OPERATION_TIMEOUT` | `5s` | Maximum duration of broker/database operations |
| `SHUTDOWN_TIMEOUT` | `15s` | gRPC graceful-shutdown deadline |
| `PROCESSING_DELAY` | `0s` | Fault injection before the receipt operation |
| `ACKNOWLEDGMENT_DELAY` | `0s` | Fault injection after successful processing, before ACK |

Duration settings accept Go duration strings, up to one minute. Required timeouts must be positive. Fault-injection delays and retry delay may be zero. Configuration errors stop the process at startup.

Keep topology settings consistent between API and consumers. Queue arguments are declared at startup; changing incompatible immutable arguments on existing queues can cause declaration failures. Changing topology should be an explicit migration or use a new queue name. A later version can manage mutable operational settings using RabbitMQ policies.

To add another queue, set `QUEUES=tasks,events`. The API accepts both; each consumer process still subscribes to its configured `QUEUE_NAME`. A separate consumer deployment can use `QUEUE_NAME=events`.

## Build and generation

The multi-stage Dockerfile downloads pinned Go modules, builds one static binary and copies it into a non-root distroless image. Generated Protocol Buffers files are part of the source tree; image builds do not generate code or install protoc.

RabbitMQ and PostgreSQL images track patch releases within the selected version lines. For reproducible deployment snapshots, pin their verified image digests and update them intentionally. This repository's initial Compose is a local learning environment rather than a deployment lockfile.
