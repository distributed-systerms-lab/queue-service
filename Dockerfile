# syntax=docker/dockerfile:1
FROM golang:1.26.9-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/queue-service ./cmd/queue-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/queue-service /queue-service
USER nonroot:nonroot
ENTRYPOINT ["/queue-service"]
CMD ["serve"]
