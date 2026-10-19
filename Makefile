MODULE := github.com/distributed-systerms-lab/queue-service

.PHONY: build test check audit generate tools integration integration-up integration-down

build:
	go build -trimpath -o bin/queue-service ./cmd/queue-service

test:
	go test -race -count=1 ./...

check:
	test -z "$$(gofmt -l cmd internal integration)"
	go vet ./...
	go test -race -count=1 ./...

audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

# Install protoc 33.5 separately. Generated files are committed, so ordinary
# builds and tests need neither protoc nor these plugins.
tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1

generate:
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc -I api \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		api/queue/v1/queue.proto

integration-up:
	docker compose -f compose.test.yaml up -d --wait

integration:
	AMQP_URL=amqp://lab:lab-test-only@127.0.0.1:5672/ \
	DATABASE_URL=postgres://lab:lab-test-only@127.0.0.1:5432/queue_lab?sslmode=disable \
	go test -race -count=1 -tags=integration -timeout=3m ./integration

integration-down:
	docker compose -f compose.test.yaml down -v
