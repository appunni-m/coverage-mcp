CARGO ?= cargo

.DEFAULT_GOAL := help

.PHONY: help build release fmt fmt-fix check check-diff clippy test coverage docs migration-parity migration-benchmark migration-status lint ci clean

help:
	@printf '%s\n' 'Coverage MCP Rust workspace'
	@printf '%s\n' '' 'Build:'
	@printf '%s\n' '  make build       cargo check for all targets' '  make release      optimized release binary'
	@printf '%s\n' '' 'Quality:'
	@printf '%s\n' '  make fmt         check rustfmt' '  make fmt-fix     apply rustfmt' '  make clippy      strict clippy with warnings denied' '  make lint        format + clippy'
	@printf '%s\n' '' 'Verification:'
	@printf '%s\n' '  make test        all workspace tests' '  make coverage    100% function/line gate + JSON evidence' '  make migration-parity  fixture-backed migration tests' '  make migration-benchmark  measured compaction workload' '  make migration-status  aggregate lane evidence and render docs' '  make docs        warnings-denied rustdoc' '  make check-diff  whitespace/error check' '  make ci          complete local gate'
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
	$(CARGO) check --workspace --all-targets --all-features --locked

check-diff:
	git diff --check

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	$(CARGO) test --workspace --all-targets --all-features --locked

coverage:
	mkdir -p target/migration
	$(CARGO) llvm-cov --offline --lib --all-features --locked \
		--ignore-filename-regex '/src/main\.rs$$' \
		--json --summary-only --output-path target/migration/coverage-raw.json \
		--fail-under-lines 100 --fail-under-functions 100 \
		--fail-uncovered-lines 0 --fail-uncovered-functions 0 -- --test-threads=1

docs:
	RUSTDOCFLAGS='-D warnings' $(CARGO) doc --workspace --all-features --no-deps --locked

migration-parity:
	mkdir -p target/migration
	$(CARGO) test --offline --test rust_migration --all-features --locked
	$(CARGO) run --offline --locked --bin migration-status -- --record-parity .

migration-benchmark:
	mkdir -p target/migration
	MIGRATION_BENCHMARK_REPORT=target/migration/benchmark-result.json $(CARGO) test --offline --test rust_migration --all-features --locked rust_compaction_benchmark_workload -- --exact --test-threads=1

migration-status:
	$(CARGO) run --offline --locked --bin migration-status -- .

lint: fmt clippy

ci:
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) migration-parity
	$(MAKE) migration-benchmark
	$(MAKE) coverage
	$(MAKE) docs
	$(MAKE) migration-status
	$(MAKE) check-diff

clean:
	$(CARGO) clean
