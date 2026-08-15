# Requires GNU Make 3.81 or newer.
CARGO ?= cargo
DUCKDB_DOWNLOAD_LIB ?= 1
FAST_FEATURES ?= --no-default-features
COVERAGE_MCP_TEST_BUILD_JOBS ?= 1

.DEFAULT_GOAL := help

.PHONY: help build release fmt fmt-fix check check-diff clippy test test-bundled test-ci-compile test-ci-lib test-ci-bins test-ci-migration-status test-ci-rust-migration test-ci-smoke test-ci coverage docs migration-parity migration-benchmark migration-status mcp-evals lint ci clean

help:
	@printf '%s\n' 'Coverage MCP Rust workspace'
	@printf '%s\n' '' 'Build:'
	@printf '%s\n' '  make build       cargo check with self-contained bundled DuckDB' '  make release      optimized self-contained release binary'
	@printf '%s\n' '' 'Quality:'
	@printf '%s\n' '  make fmt         check rustfmt' '  make fmt-fix     apply rustfmt' '  make clippy      strict clippy with warnings denied' '  make lint        format + clippy'
	@printf '%s\n' '' 'Verification:'
	@printf '%s\n' '  make test        all workspace tests with exact prebuilt DuckDB' '  make test-bundled  all tests with self-contained bundled DuckDB' '  make test-ci-compile  compile every CI test target with one Cargo job' '  make test-ci     run each CI test target serially with retained diagnostics' '  make coverage    100% function/line gate + JSON evidence' '  make migration-parity  fixture-backed migration tests' '  make migration-benchmark  measured compaction workload' '  make migration-status  aggregate lane evidence and render docs' '  make mcp-evals  opt-in MCP usability/safety/efficiency evaluation (not CI)' '  make docs        warnings-denied rustdoc' '  make check-diff  whitespace/error check' '  make ci          complete local gate'
	@printf '%s\n' '' 'Fast verification:'
	@printf '%s\n' '  DUCKDB_DOWNLOAD_LIB=1 downloads the exact matching DuckDB release once.' '  COVERAGE_MCP_TEST_BUILD_JOBS=1 bounds both CI test phases; raise it only on memory-rich hosts.' '  Override FAST_FEATURES or set DUCKDB_DOWNLOAD_LIB=0 only with a compatible system DuckDB.'
	@printf '%s\n' '' 'Runtime:'
	@printf '%s\n' '  cargo run -- serve' '  cargo run -- connect --repo .' '  cargo run -- compact --repo .'

build:
	$(CARGO) check --workspace --all-targets --all-features --locked

release:
	$(CARGO) build --release --locked

fmt:
	$(CARGO) fmt --all -- --check

fmt-fix:
	$(CARGO) fmt --all

check:
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) check --workspace --all-targets $(FAST_FEATURES) --locked

check-diff:
	git diff --check

clippy:
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) clippy --workspace --all-targets $(FAST_FEATURES) --locked -- -D warnings

test:
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) test --workspace --all-targets $(FAST_FEATURES) --locked

test-bundled:
	$(CARGO) test --workspace --all-targets --all-features --locked

test-ci-compile:
	mkdir -p target/migration
	rm -f target/migration/test-*.log
	CARGO_BUILD_JOBS=$(COVERAGE_MCP_TEST_BUILD_JOBS) DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) test --workspace --all-targets $(FAST_FEATURES) --locked --no-run

define run_ci_test
	@set -eu; \
	mkdir -p target/migration; \
	log="target/migration/test-$(2).log"; \
	if MIGRATION_BENCHMARK_REPORT=target/migration/benchmark-result.json \
		CARGO_BUILD_JOBS=$(COVERAGE_MCP_TEST_BUILD_JOBS) DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) test --workspace $(1) $(FAST_FEATURES) --locked -- --test-threads=1 \
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
	$(call run_ci_test,--test rust_migration,rust-migration)

test-ci-smoke:
	$(call run_ci_test,--test smoke,smoke)

test-ci:
	$(MAKE) test-ci-lib
	$(MAKE) test-ci-bins
	$(MAKE) test-ci-migration-status
	$(MAKE) test-ci-rust-migration
	$(MAKE) test-ci-smoke

coverage:
	mkdir -p target/migration
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) llvm-cov --lib $(FAST_FEATURES) --locked \
		--ignore-filename-regex '/src/main\.rs$$' \
		--json --summary-only --output-path target/migration/coverage-raw.json \
		--fail-under-lines 100 --fail-under-functions 100 \
		--fail-uncovered-lines 0 --fail-uncovered-functions 0 -- --test-threads=1

docs:
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) RUSTDOCFLAGS='-D warnings' $(CARGO) doc --workspace $(FAST_FEATURES) --no-deps --locked

migration-parity:
	mkdir -p target/migration
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) test --test rust_migration $(FAST_FEATURES) --locked
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) run $(FAST_FEATURES) --locked --bin migration-status -- --record-parity .

migration-benchmark:
	mkdir -p target/migration
	MIGRATION_BENCHMARK_REPORT=target/migration/benchmark-result.json DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) test --test rust_migration $(FAST_FEATURES) --locked rust_compaction_benchmark_workload -- --exact --test-threads=1

migration-status:
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) run $(FAST_FEATURES) --locked --bin migration-status -- .

mcp-evals:
	mkdir -p target/evals
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) run $(FAST_FEATURES) --locked --bin mcp-evals -- --report target/evals/mcp-eval-report.json

lint: fmt clippy

ci:
	$(MAKE) lint
	$(MAKE) test-ci-compile
	$(MAKE) test-ci
	DUCKDB_DOWNLOAD_LIB=$(DUCKDB_DOWNLOAD_LIB) $(CARGO) run $(FAST_FEATURES) --locked --bin migration-status -- --record-parity .
	$(MAKE) coverage
	$(MAKE) docs
	$(MAKE) migration-status
	$(MAKE) check-diff

clean:
	$(CARGO) clean
