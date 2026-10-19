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

## Executed successfully in GitHub Actions

The [successful CI run](https://github.com/distributed-systerms-lab/queue-service/actions/runs/39331232300) verified implementation commit `2295bf0f1f6b087ac3c06894181c86645b77da82` with:

- Formatting, `go vet`, and unit tests with the race detector.
- The pinned govulncheck scan.
- Real RabbitMQ 4.3 and PostgreSQL 17 dependencies started through Docker Compose.
- Integration tests with the race detector, including concurrent duplicate deliveries, envelope preservation, dead-letter transfer, unroutable publication, conflicting identities and killing an actual consumer process after commit but before ACK.
- Application Compose configuration validation.
- Building the multi-stage, non-root runtime image.

The application Compose has not been exercised as a complete deployment here. CI tests the real broker/database flow through Go processes and builds the application image separately. The manual restart and scaling experiments remain documented in `failure-experiments.md`.

## Repository publication

The implementation, tests and documentation are published on the repository's `main` branch. The GitHub App installation on the organization resolved the earlier integration authorization failure. The documentation-only commit recording these results does not change the verified implementation.
