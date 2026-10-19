# Failure experiments

Run these from the repository root after the README startup steps. Use fresh message IDs when repeating an experiment, because receipts survive container restarts.

## 1. Publish the same message twice

```sh
docker compose exec queue-service /queue-service publish -id duplicate-1 -payload same
docker compose exec queue-service /queue-service publish -id duplicate-1 -payload same
docker compose exec postgres psql -U lab -d queue_lab -c \
  "SELECT message_id, count(*) FROM processed_messages WHERE message_id = 'duplicate-1' GROUP BY message_id;"
```

Expect one row with count one. The consumer logs `new_receipt:true` once, and `new_receipt:false` on the duplicate. Both deliveries can be acknowledged.

Publishing `duplicate-1` again with a different payload creates an identity conflict. The original receipt remains unchanged and the conflicting delivery reaches `tasks.dead`.

## 2. Exhaust retries

```sh
docker compose exec queue-service /queue-service publish -id failure-1 -type lab.fail
docker compose logs -f consumer
```

With the defaults, observe three retry decisions and then a dead-letter decision after the fourth processing attempt. Inspect after those attempts finish:

```sh
docker compose exec queue-service /queue-service inspect -queue tasks
docker compose exec queue-service /queue-service peek-dead -queue tasks
```

`peek-dead` may return any existing dead letter, so start with an empty DLQ if you need to identify this exact message. It does not remove messages. Broker death metadata appears under `headers`.

## 3. Kill after commit, before ACK

Use a single consumer process so the target is unambiguous:

```sh
ACKNOWLEDGMENT_DELAY=30s docker compose up -d --force-recreate --scale consumer=1 consumer
docker compose exec queue-service /queue-service publish -id crash-window-1 -payload survives
docker compose logs -f consumer
```

Wait for `message processed` for `crash-window-1`, then immediately run in another terminal:

```sh
docker compose kill -s SIGKILL consumer
ACKNOWLEDGMENT_DELAY=0s docker compose up -d --force-recreate consumer
docker compose logs -f consumer
```

Expect the replacement to log `redelivered:true` and `new_receipt:false`, then ACK. The receipt count remains one:

```sh
docker compose exec postgres psql -U lab -d queue_lab -c \
  "SELECT count(*) FROM processed_messages WHERE message_id = 'crash-window-1';"
```

The integration suite automates this exact process-death window with a real child process.

## 4. Restart the broker with a waiting message

```sh
docker compose stop consumer
docker compose exec queue-service /queue-service publish -id persistent-1 -payload waiting
docker compose restart rabbitmq
docker compose up -d --wait rabbitmq queue-service
docker compose exec queue-service /queue-service inspect -queue tasks
docker compose up -d consumer
```

Before starting the consumer, the message remains ready after broker recovery. Starting the consumer records it. The named broker volume and stable node identity retain the durable queue and confirmed message.

## 5. Run competing consumers

```sh
docker compose up -d --scale consumer=2
docker compose exec queue-service /queue-service inspect -queue tasks
```

With default concurrency, the queue reports eight consumer subscriptions: four per process. Publish several messages and inspect consumer logs. Each delivery is assigned to a consumer; requeue and timing can change the order. All replicas use the same PostgreSQL primary key to suppress duplicate effects.

```sh
docker compose up -d --scale consumer=1
```

## 6. Stop the broker

```sh
docker compose stop rabbitmq
docker compose logs queue-service consumer
```

The processes detect lost connections and exit; Docker attempts to restart them. An API request during this period may receive an unavailable error or find no listening API process. Once RabbitMQ returns, process restarts establish fresh connections:

```sh
docker compose up -d --wait
```

If a publish had an uncertain outcome, retry with the original ID and identical envelope. Creating a new ID would bypass duplicate suppression.
