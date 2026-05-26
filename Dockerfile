# syntax=docker/dockerfile:1

FROM golang:1.25-bookworm AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum ./cmd/velum && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum-api ./cmd/velum-api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum-history ./cmd/velum-history && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum-matcher ./cmd/velum-matcher && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum-scheduler ./cmd/velum-scheduler && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum-migrate ./cmd/velum-migrate && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/velum-worker ./cmd/velum-worker

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=builder /out/velum /app/velum
COPY --from=builder /out/velum-api /app/velum-api
COPY --from=builder /out/velum-history /app/velum-history
COPY --from=builder /out/velum-matcher /app/velum-matcher
COPY --from=builder /out/velum-scheduler /app/velum-scheduler
COPY --from=builder /out/velum-migrate /app/velum-migrate
COPY --from=builder /out/velum-worker /app/velum-worker

ENV VELUM_HTTP_ADDR=:8080
ENV VELUM_GRPC_ADDR=:9090
ENV VELUM_HISTORY_GRPC_ADDR=:9091
EXPOSE 8080 9090 9091

USER nonroot:nonroot
ENTRYPOINT ["/app/velum"]
