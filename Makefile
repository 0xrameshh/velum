.PHONY: help proto build build-worker test run run-api run-matcher run-scheduler up down logs curl-start curl-status curl-delayed-start curl-delayed-status curl-saga-start curl-saga-fail curl-saga-status clean

help:
	@echo "Targets: proto build test run up down logs smoke curl-* (see Makefile)"

API_URL ?= http://localhost:$(or $(VELUM_HOST_HTTP_PORT),8080)

proto:
	protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		proto/velum/v1/worker.proto \
		proto/velum/v1/history.proto

BINARIES := velum velum-api velum-history velum-matcher velum-scheduler velum-migrate velum-worker

build:
	@mkdir -p bin
	@for b in $(BINARIES); do go build -o bin/$$b ./cmd/$$b; done

build-worker:
	go build -o bin/velum-worker ./cmd/velum-worker

test:
	go test ./...

run: build
	VELUM_ENABLE_EMBEDDED_WORKER=true \
	VELUM_DATABASE_URL="postgres://velum:velum@localhost:5432/velum?sslmode=disable" \
	./bin/velum

run-history: build
	VELUM_DATABASE_URL="postgres://velum:velum@localhost:5432/velum?sslmode=disable" \
	VELUM_HISTORY_GRPC_ADDR=":9091" \
	./bin/velum-history

run-api: build
	VELUM_HISTORY_GRPC_ADDR="localhost:9091" \
	./bin/velum-api

run-matcher: build
	VELUM_DATABASE_URL="postgres://velum:velum@localhost:5432/velum?sslmode=disable" \
	VELUM_HISTORY_GRPC_ADDR="localhost:9091" \
	./bin/velum-matcher

run-scheduler: build
	VELUM_DATABASE_URL="postgres://velum:velum@localhost:5432/velum?sslmode=disable" \
	VELUM_HISTORY_GRPC_ADDR="localhost:9091" \
	./bin/velum-scheduler

run-worker-default: build-worker
	VELUM_GRPC_ADDR=localhost:9090 \
	VELUM_TASK_QUEUE=default \
	VELUM_WORKER_ID=local-default \
	./bin/velum-worker

run-worker-email: build-worker
	VELUM_GRPC_ADDR=localhost:9090 \
	VELUM_TASK_QUEUE=email \
	VELUM_WORKER_ID=local-email \
	./bin/velum-worker

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f velum-history velum-api velum-matcher-default velum-matcher-email velum-matcher-payments velum-scheduler worker-default worker-email worker-payments redis

smoke: up
	@echo "waiting for stack..."
	@sleep 8
	@$(MAKE) curl-start
	@sleep 2
	@$(MAKE) curl-status
	@$(MAKE) curl-delayed-start
	@sleep 8
	@$(MAKE) curl-delayed-status
	@$(MAKE) curl-saga-start
	@sleep 3
	@$(MAKE) curl-saga-status
	@echo "smoke tests finished"

curl-start:
	curl -sS -X POST "$(API_URL)/api/v1/namespaces/default/workflows/greet/start" \
		-H "Content-Type: application/json" \
		-d '{"input":{"name":"Ramesh"}}' | tee /tmp/velum-run.json
	@echo

curl-status:
	@RUN_ID=$$(python3 -c "import json; print(json.load(open('/tmp/velum-run.json'))['run_id'])"); \
		curl -sS "$(API_URL)/api/v1/namespaces/default/runs/$$RUN_ID" | python3 -m json.tool

curl-delayed-start:
	curl -sS -X POST "$(API_URL)/api/v1/namespaces/default/workflows/delayed_greet/start" \
		-H "Content-Type: application/json" \
		-d '{"input":{"name":"Ramesh","sleep_seconds":5}}' | tee /tmp/velum-delayed-run.json
	@echo

curl-delayed-status:
	@RUN_ID=$$(python3 -c "import json; print(json.load(open('/tmp/velum-delayed-run.json'))['run_id'])"); \
		curl -sS "$(API_URL)/api/v1/namespaces/default/runs/$$RUN_ID" | python3 -m json.tool

curl-saga-start:
	curl -sS -X POST "$(API_URL)/api/v1/namespaces/default/workflows/order_saga/start" \
		-H "Content-Type: application/json" \
		-d '{"input":{"order_id":"ord-42","fail_ship":false}}' | tee /tmp/velum-saga-run.json
	@echo

curl-saga-fail:
	curl -sS -X POST "$(API_URL)/api/v1/namespaces/default/workflows/order_saga/start" \
		-H "Content-Type: application/json" \
		-d '{"input":{"order_id":"ord-fail","fail_ship":true}}' | tee /tmp/velum-saga-run.json
	@echo

curl-saga-status:
	@RUN_ID=$$(python3 -c "import json; print(json.load(open('/tmp/velum-saga-run.json'))['run_id'])"); \
		curl -sS "$(API_URL)/api/v1/namespaces/default/runs/$$RUN_ID" | python3 -m json.tool

clean:
	rm -rf bin/
	docker compose down -v
