# Requires GNU Make 3.81 or newer.
CARGO ?= cargo
DUCKDB_DOWNLOAD_LIB ?= 1
FAST_FEATURES ?= --no-default-features
COVERAGE_MCP_TEST_BUILD_JOBS ?= 1
RUST_MIGRATION_TEST ?=
MIGRATION_DIR := target/migration
EVAL_DIR := target/evals

CARGO_ENV = DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB)
FAST_CARGO = $(CARGO_ENV) $(CARGO)
FAST_CARGO_FLAGS = $(FAST_FEATURES) --locked
SERIAL_TEST_ARGS = -- --test-threads=1
CI_CARGO_ENV = CARGO_BUILD_JOBS=$(COVERAGE_MCP_TEST_BUILD_JOBS) $(CARGO_ENV)
CI_CARGO = env $(CI_CARGO_ENV) $(CARGO)
MIGRATION_STATUS = $(FAST_CARGO) run $(FAST_CARGO_FLAGS) --bin migration-status --

.DEFAULT_GOAL := help

.PHONY: help build release fmt fmt-fix check check-diff clippy test test-bundled test-ci-compile test-ci-lib test-ci-bins test-ci-migration-status test-ci-rust-migration test-ci-benchmark test-ci-smoke test-ci coverage docs migration-parity migration-benchmark migration-status mcp-evals lint ci clean

help:
	@printf '%s\n' 'Coverage MCP Rust workspace'
	@printf '%s\n' '' 'Build:'
	@printf '%s\n' '  make build       cargo check with self-contained bundled DuckDB' '  make release      optimized self-contained release binary'
	@printf '%s\n' '' 'Quality:'
	@printf '%s\n' '  make fmt         check rustfmt' '  make fmt-fix     apply rustfmt' '  make clippy      strict clippy with warnings denied' '  make lint        format + clippy'
	@printf '%s\n' '' 'Verification:'
	@printf '%s\n' '  make test        all workspace tests with exact prebuilt DuckDB' '  make test-bundled  all tests with self-contained bundled DuckDB' '  make test-ci-compile  compile every CI test target with one Cargo job' '  make test-ci     run each CI test target serially with retained diagnostics' '  make coverage    100% region/line/function gate + JSON evidence' '  make migration-parity  fixture-backed migration tests' '  make migration-benchmark  measured compaction workload' '  make migration-status  aggregate lane evidence and render docs' '  make mcp-evals  opt-in MCP usability/safety/efficiency evaluation (not CI)' '  make docs        warnings-denied rustdoc' '  make check-diff  whitespace/error check' '  make ci          complete local gate'
	@printf '%s\n' '' 'Fast verification:'
	@printf '%s\n' '  DUCKDB_DOWNLOAD_LIB=1 downloads the exact matching DuckDB release once.' '  COVERAGE_MCP_TEST_BUILD_JOBS=1 bounds both CI test phases; raise it only on memory-rich hosts.' '  Set RUST_MIGRATION_TEST to run one exact migration test in its own process.' '  Override FAST_FEATURES or set DUCKDB_DOWNLOAD_LIB=0 only with a compatible system DuckDB.'
	@printf '%s\n' '' 'Runtime:'
	@printf '%s\n' '  cargo run --package coverage-mcp -- serve' '  cargo run --package coverage-mcp -- connect --repo .' '  cargo run --package coverage-mcp -- compact --repo .'

build:
	$(CARGO) check --workspace --all-targets --all-features --locked

release:
	$(CARGO) build --release --locked

fmt:
	$(CARGO) fmt --all -- --check

fmt-fix:
	$(CARGO) fmt --all

check:
	$(FAST_CARGO) check --workspace --all-targets $(FAST_CARGO_FLAGS)

check-diff:
	git diff --check

clippy:
	$(FAST_CARGO) clippy --workspace --all-targets $(FAST_CARGO_FLAGS) -- -D warnings

test:
	$(FAST_CARGO) test --workspace --all-targets $(FAST_CARGO_FLAGS) $(SERIAL_TEST_ARGS)

test-bundled:
	$(CARGO) test --workspace --all-targets --all-features --locked -- --test-threads=1

test-ci-compile:
	rm -f "$(MIGRATION_DIR)"/test-*.log
	$(CI_CARGO) test --workspace --all-targets $(FAST_CARGO_FLAGS) --no-run

define run_ci_test
	@set -eu; \
	mkdir -p "$(MIGRATION_DIR)"; \
	log="$(MIGRATION_DIR)/test-$(2).log"; \
	if env MIGRATION_BENCHMARK_REPORT="$(MIGRATION_DIR)/benchmark-result.json" $(CI_CARGO_ENV) $(CARGO) test --workspace $(1) $(FAST_CARGO_FLAGS) $(SERIAL_TEST_ARGS) $(3) \
		>"$$log" 2>&1; then \
		printf 'passed %s\n' '$(2)'; \
	else \
		code=$$?; \
		printf 'failed %s (exit %s)\n' '$(2)' "$$code" >&2; \
		tail -n 200 "$$log" >&2 || true; \
		exit "$$code"; \
	fi
endef

test-ci-lib:
	$(call run_ci_test,--lib,lib)

test-ci-bins:
	$(call run_ci_test,--bins,bins)

test-ci-migration-status:
	$(call run_ci_test,--test migration_status,migration-status)

test-ci-rust-migration:
	$(if $(strip $(RUST_MIGRATION_TEST)),$(call run_ci_test,--test rust_migration $(RUST_MIGRATION_TEST),rust-migration-$(RUST_MIGRATION_TEST),--exact),$(call run_ci_test,--test rust_migration,rust-migration,--skip rust_compaction_benchmark_workload))

test-ci-benchmark:
	$(call run_ci_test,--test rust_migration rust_compaction_benchmark_workload,benchmark,--exact)

test-ci-smoke:
	$(call run_ci_test,--test smoke,smoke)

test-ci:
	$(MAKE) test-ci-lib
	$(MAKE) test-ci-bins
	$(MAKE) test-ci-migration-status
	$(MAKE) test-ci-rust-migration
	$(MAKE) test-ci-benchmark
	$(MAKE) test-ci-smoke

coverage:
	mkdir -p "$(MIGRATION_DIR)"
	$(FAST_CARGO) llvm-cov --lib $(FAST_CARGO_FLAGS) \
		--ignore-filename-regex '/src/main\.rs$$' \
		--json --summary-only --output-path "$(MIGRATION_DIR)/coverage-raw.json" \
		--fail-under-lines 100 --fail-under-functions 100 --fail-under-regions 100 \
		--fail-uncovered-lines 0 --fail-uncovered-functions 0 --fail-uncovered-regions 0 -- --test-threads=1

docs:
	RUSTDOCFLAGS='-D warnings' $(FAST_CARGO) doc --workspace $(FAST_CARGO_FLAGS) --no-deps

migration-parity:
	$(FAST_CARGO) test --test rust_migration $(FAST_CARGO_FLAGS)
	$(MIGRATION_STATUS) --record-parity .

migration-benchmark:
	mkdir -p "$(MIGRATION_DIR)"
	MIGRATION_BENCHMARK_REPORT="$(MIGRATION_DIR)/benchmark-result.json" $(FAST_CARGO) test --test rust_migration $(FAST_CARGO_FLAGS) rust_compaction_benchmark_workload -- --exact --test-threads=1

migration-status:
	$(MIGRATION_STATUS) .

mcp-evals:
	mkdir -p "$(EVAL_DIR)"
	$(FAST_CARGO) run $(FAST_CARGO_FLAGS) --bin mcp-evals -- --report "$(EVAL_DIR)/mcp-eval-report.json"

lint: fmt clippy

ci:
	$(MAKE) lint
	$(MAKE) test-ci-compile
	$(MAKE) test-ci
	$(MIGRATION_STATUS) --record-parity .
	$(MAKE) coverage
	$(MAKE) docs
	$(MAKE) migration-status
	$(MAKE) check-diff

clean:
	$(CARGO) clean
