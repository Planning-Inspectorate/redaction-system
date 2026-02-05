FUNC_PORT ?= 7071
FUNC_BASE_URL ?= http://localhost:$(FUNC_PORT)

PERF_TOTAL ?= 5
PERF_CONCURRENCY ?= 5
PERF_TIMEOUT_S ?= 7200

run:
	@echo "Starting Azurite..."
	azurite > /tmp/azurite.log 2>&1 &
	@echo "Starting Azure Functions on port $(FUNC_PORT)..."
	cd redactor && func start --port $(FUNC_PORT)

trigger:
	python3 scripts/trigger_redaction.py

wait-func:
	@echo "Checking Functions host at $(FUNC_BASE_URL)..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		if curl -sS $(FUNC_BASE_URL) > /dev/null 2>&1; then \
			echo "Functions host is reachable at $(FUNC_BASE_URL)"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "Functions host is not reachable at $(FUNC_BASE_URL)."; \
	echo "Did you run 'make run' in another terminal?"; \
	exit 1

e2e: wait-func
	@echo "Running e2e tests (expects Functions already running)..."
	cd redactor && \
		export PYTHONPATH=$$(pwd) && \
		export E2E_FUNCTION_BASE_URL=$(FUNC_BASE_URL) && \
		export E2E_SKIP_REDACTION=false && \
		pytest -m e2e -vv -rP

perf: wait-func
	@echo "Running perf tests (expects Functions already running)..."
	cd redactor && \
		export PYTHONPATH=$$(pwd) && \
		PERF_TOTAL=$(PERF_TOTAL) PERF_CONCURRENCY=$(PERF_CONCURRENCY) PERF_TIMEOUT_S=$(PERF_TIMEOUT_S) \
		python3 -m pytest -q -s test/perf_test/test_perf_concurrent_redactions.py


