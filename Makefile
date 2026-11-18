# listing-alerts: one command per task. `make help` lists them.

# Docker Desktop on macOS does not always put its CLI on PATH.
DOCKER  := $(shell command -v docker 2>/dev/null || echo /Applications/Docker.app/Contents/Resources/bin/docker)
COMPOSE := $(DOCKER) compose
# ...and its credential helpers must be on PATH for the docker CLI itself.
export PATH := $(PATH):$(dir $(DOCKER))
GO_PKGS := ./...

.DEFAULT_GOAL := help

.PHONY: help
help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z][a-z-]*:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.env:
	@cp .env.example .env
	@sed -i.bak -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$$(openssl rand -hex 16)/" \
	            -e "s/^GRAFANA_ADMIN_PASSWORD=.*/GRAFANA_ADMIN_PASSWORD=$$(openssl rand -hex 12)/" .env && rm -f .env.bak
	@echo "Created .env with random local passwords (gitignored)."

.PHONY: env
env: .env ## Create .env with random local passwords if it does not exist

.PHONY: up
up: .env ## Build and start the whole stack, then wait until it is ready
	$(COMPOSE) up -d --build --wait
	@curl -fsS -X POST http://localhost:9091/-/reload >/dev/null || true
	@./scripts/smoke.sh
	@echo
	@echo "  API        http://localhost:8081/readyz"
	@echo "  Grafana    http://localhost:3300  (admin / GRAFANA_ADMIN_PASSWORD in .env)"
	@echo "  Prometheus http://localhost:9091"
	@echo "  Redpanda   http://localhost:8180  (Console)   localhost:19092 (Kafka API)"
	@echo "  Mailpit    http://localhost:8025"

.PHONY: live
live: .env tokens ## Start the stack plus the PropTx and off-MLS pollers
	@mkdir -p data/recordings data/offmls
	@test -f data/offmls/private-listings.json || echo '[]' > data/offmls/private-listings.json
	$(COMPOSE) --profile live up -d --build --wait
	@curl -fsS -X POST http://localhost:9091/-/reload >/dev/null || true
	@./scripts/smoke.sh
	@echo "  Pollers: make logs-live   Dashboards: http://localhost:3300/d/la-ingest  http://localhost:3300/d/la-detect"

.PHONY: logs-live
logs-live: ## Follow poller logs
	$(COMPOSE) --profile live logs -f --tail=50 poller-vow poller-idx poller-offmls

.PHONY: events
events: ## Print the latest listing events (time, type, listing, prices, visibility)
	@$(COMPOSE) exec -T redpanda rpk topic consume listing.events -o -25 -n 25 -f '%v\n' | ./scripts/events.py

.PHONY: down
down: ## Stop the stack (keeps data)
	$(COMPOSE) --profile live down

.PHONY: clean
clean: ## Stop the stack and delete its volumes
	$(COMPOSE) --profile live down -v

.PHONY: logs
logs: ## Follow service logs
	$(COMPOSE) logs -f --tail=100

.PHONY: smoke
smoke: ## Check a running stack end to end
	@./scripts/smoke.sh

.PHONY: test
test: ## Unit tests with the race detector
	go test -race -count=1 $(GO_PKGS)

.PHONY: itest
itest: .env ## Integration tests against the running stack (make up first)
	@set -a; . ./.env; set +a; \
	POSTGRES_URL="postgres://$${POSTGRES_USER}:$${POSTGRES_PASSWORD}@localhost:5433/$${POSTGRES_DB}?sslmode=disable" \
	KAFKA_BROKERS=localhost:19092 \
	go test -race -count=1 -tags integration ./...

.PHONY: tokens
tokens: .env ## Copy the PropTx tokens from the macOS Keychain into .env (never printed)
	@./scripts/tokens.sh

HOTRENTALS_DIR ?= $(HOME)/Projects/Personal/hotrentals

.PHONY: geodata
geodata: ## Copy hotrentals' Ontario address points (municipal open data) for geocoding
	@mkdir -p data/geo
	@test -f $(HOTRENTALS_DIR)/data/address-points.db || (echo "no address points in $(HOTRENTALS_DIR)/data; set HOTRENTALS_DIR" && exit 1)
	cp $(HOTRENTALS_DIR)/data/address-points.db data/geo/address-points.db

.PHONY: apikey
apikey: .env ## Create a site API key: make apikey SITE=mertmetin (printed once; only its hash is stored)
	@test -n "$(SITE)" || (echo "usage: make apikey SITE=<name>" && exit 1)
	@set -a; . ./.env; set +a; \
	POSTGRES_URL="postgres://$${POSTGRES_USER}:$${POSTGRES_PASSWORD}@localhost:5433/$${POSTGRES_DB}?sslmode=disable" \
	go run ./cmd/apikey -site $(SITE) -label "$(LABEL)"

.PHONY: matchbench
matchbench: ## Matcher benchmark: 1M synthetic saved searches (about a minute, ~3 GB RAM)
	go run ./cmd/matchbench -searches 1000000 -events 200000 -shards 5

.PHONY: lint
lint: ## go vet and gofmt check
	go vet $(GO_PKGS)
	@test -z "$$(gofmt -l .)" || (gofmt -l .; echo "run gofmt -w ."; exit 1)

.PHONY: secrets
secrets: ## Scan git history and staged changes for secrets (.env is gitignored, so not scanned)
	gitleaks git --redact --no-banner .
	gitleaks git --staged --redact --no-banner .

.PHONY: hooks
hooks: ## Install the pre-commit secret scan
	git config core.hooksPath scripts/hooks
	@echo "pre-commit hook installed (gitleaks on staged changes)."

.PHONY: check
check: lint test secrets ## Everything CI runs
