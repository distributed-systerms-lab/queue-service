# Verification record

Prepared on 2026-10-19 using Go 1.26.9 on Linux amd64.

## Executed successfully

- `make check`: gofmt verification, `go vet ./...`, and `go test -race -count=1 ./...`.
- `make build`: builds the CLI, gRPC server and consumer executable.
- `make audit`: govulncheck 1.9.0 reports no vulnerabilities after dependency updates.
- `go test -tags=integration -run '^$' ./integration`: compiles the integration suite without executing dependency-backed tests.
- `docker-compose -f compose.yaml config --quiet`, with validation-only passwords.
- `docker-compose -f compose.test.yaml config --quiet`.
- `git diff --check`.

Compose validation used the standalone Docker Compose CLI. It did not start containers.

## Included but not executed here

The integration suite exercises actual RabbitMQ and PostgreSQL, including concurrent duplicate deliveries, envelope preservation, dead-letter transfer, unroutable publication, conflicting identities and killing a consumer after commit but before ACK.

These tests require `make integration-up` followed by `make integration`. This preparation environment has no Docker engine. The runtime image has not been built here, and the GitHub Actions workflow has not run.

## Repository publication

The repository is readable. Both Git tree creation and file creation returned `403: Resource not accessible by integration`. No files were uploaded and the remote main branch was left unchanged. Local commits retain the implementation for publication after GitHub access is corrected.
