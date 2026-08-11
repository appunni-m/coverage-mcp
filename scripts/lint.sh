#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo '== rustfmt =='
cargo fmt --all -- --check

echo '== clippy =='
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

echo '== tests =='
cargo test --workspace --all-targets --all-features --locked

echo '== coverage =='
cargo llvm-cov --offline --lib --all-features --locked \
  --ignore-filename-regex '/src/main\.rs$' \
  --fail-under-lines 100 --fail-under-functions 100 \
  --fail-uncovered-lines 0 --fail-uncovered-functions 0 -- --test-threads=1

echo '== rustdoc =='
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked

echo '== diff =='
git diff --check
