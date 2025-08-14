SHELL := /bin/bash

.PHONY: build up down logs test test-docker smoke

build:
	docker compose build app

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100 app

# Run tests in a Go container (avoids local toolchain issues)
test-docker:
	docker run --rm -v $(PWD):/workspace -w /workspace golang:1.23 bash -lc 'go mod download && go test ./...'

smoke:
	./scripts/smoke.sh


