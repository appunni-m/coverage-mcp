CARGO ?= cargo

.DEFAULT_GOAL := help

.PHONY: help build release fmt fmt-fix check check-diff clippy test test-ci coverage docs migration-parity migration-benchmark migration-status lint ci clean

help:
	@printf '%s\n' 'Coverage MCP Rust workspace'
	@printf '%s\n' '' 'Build:'
	@printf '%s\n' '  make build       cargo check for all targets' '  make release      optimized release binary'
	@printf '%s\n' '' 'Quality:'
	@printf '%s\n' '  make fmt         check rustfmt' '  make fmt-fix     apply rustfmt' '  make clippy      strict clippy with warnings denied' '  make lint        format + clippy'
	@printf '%s\n' '' 'Verification:'
	@printf '%s\n' '  make test        all workspace tests' '  make test-ci     compile all targets once, then run test targets concurrently' '  make coverage    100% function/line gate + JSON evidence' '  make migration-parity  fixture-backed migration tests' '  make migration-benchmark  measured compaction workload' '  make migration-status  aggregate lane evidence and render docs' '  make docs        warnings-denied rustdoc' '  make check-diff  whitespace/error check' '  make ci          complete local gate'
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

test-ci:
	@set -eu; \
	mkdir -p target/migration; \
	rm -f target/migration/test-*.log; \
	$(CARGO) test --workspace --all-targets --all-features --locked --no-run; \
	run_harness() { name=$$1; shift; log="target/migration/test-$$name.log"; if "$$@" >"$$log" 2>&1; then printf 'passed %s\n' "$$name"; else code=$$?; printf 'failed %s (exit %s)\n' "$$name" "$$code" >&2; tail -n 120 "$$log" >&2 || true; return "$$code"; fi; }; \
	run_harness lib $(CARGO) test --workspace --lib --all-features --locked -- --test-threads=1 & p1=$$!; \
	run_harness rust-migration env MIGRATION_BENCHMARK_REPORT=target/migration/benchmark-result.json $(CARGO) test --workspace --test rust_migration --all-features --locked -- --test-threads=1 & p2=$$!; \
	run_harness migration-status $(CARGO) test --workspace --test migration_status --all-features --locked -- --test-threads=1 & p3=$$!; \
	run_harness smoke $(CARGO) test --workspace --test smoke --all-features --locked -- --test-threads=1 & p4=$$!; \
	run_harness coverage-mcp $(CARGO) test --workspace --bin coverage-mcp --all-features --locked -- --test-threads=1 & p5=$$!; \
	run_harness migration-status-bin $(CARGO) test --workspace --bin migration-status --all-features --locked -- --test-threads=1 & p6=$$!; \
	status=0; \
	if ! wait "$$p1"; then status=1; fi; \
	if ! wait "$$p2"; then status=1; fi; \
	if ! wait "$$p3"; then status=1; fi; \
	if ! wait "$$p4"; then status=1; fi; \
	if ! wait "$$p5"; then status=1; fi; \
	if ! wait "$$p6"; then status=1; fi; \
	exit "$$status"

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
	$(CARGO) build --offline --locked --bin migration-status
	./target/debug/migration-status .

lint: fmt clippy

ci:
	$(MAKE) lint
	$(MAKE) test-ci
	./target/debug/migration-status --record-parity .
	$(MAKE) coverage
	$(MAKE) docs
	$(MAKE) migration-status
	$(MAKE) check-diff

clean:
	$(CARGO) clean
