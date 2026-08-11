CARGO ?= cargo

.DEFAULT_GOAL := help

.PHONY: help build release fmt fmt-fix check clippy test coverage docs lint ci clean

help:
	@printf '%s\n' 'Coverage MCP Rust workspace'
	@printf '%s\n' '' 'Build:'
	@printf '%s\n' '  make build       cargo check for all targets' '  make release      optimized release binary'
	@printf '%s\n' '' 'Quality:'
	@printf '%s\n' '  make fmt         check rustfmt' '  make fmt-fix     apply rustfmt' '  make clippy      strict clippy with warnings denied' '  make lint        format + clippy'
	@printf '%s\n' '' 'Verification:'
	@printf '%s\n' '  make test        all workspace tests' '  make coverage    100% function/line gate' '  make docs        warnings-denied rustdoc' '  make ci          complete local gate'
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

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	$(CARGO) test --workspace --all-targets --all-features --locked

coverage:
	$(CARGO) llvm-cov --offline --lib --all-features --locked \
		--ignore-filename-regex '/src/main\.rs$$' \
		--fail-under-lines 100 --fail-under-functions 100 \
		--fail-uncovered-lines 0 --fail-uncovered-functions 0 -- --test-threads=1

docs:
	RUSTDOCFLAGS='-D warnings' $(CARGO) doc --workspace --all-features --no-deps --locked

lint: fmt clippy

ci: lint test coverage docs

clean:
	$(CARGO) clean
