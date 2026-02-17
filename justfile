#!/usr/bin/env just --justfile

# if running in CI, treat warnings as errors
ci_mode := if env('CI', '') != '' {'1'} else {''}
export RUSTFLAGS := env('RUSTFLAGS', if ci_mode == '1' {'-D warnings'} else {''})
export RUSTDOCFLAGS := env('RUSTDOCFLAGS', if ci_mode == '1' {'-D warnings'} else {''})
export RUST_BACKTRACE := env('RUST_BACKTRACE', if ci_mode == '1' {'1'} else {''})

@_default:
    {{just_executable()}} --list

# Build the project
build:
    cargo build --release

# Quick compile without building a binary
check:
    cargo check --all-targets

# Run all tests as expected by CI
ci-test: env-info test-fmt clippy test build

# Clean all build artifacts
clean:
    cargo clean

# Run cargo clippy to lint the code
clippy *args:
    cargo clippy --all-targets {{args}}

# Print environment info
env-info:
    @echo "Running {{if ci_mode == '1' {'in CI mode'} else {'in dev mode'} }} on {{os()}} / {{arch()}}"
    @echo "PWD $(pwd)"
    {{just_executable()}} --version
    rustc --version
    cargo --version

# Reformat all code
fmt:
    cargo fmt --all

# Run all unit tests
test:
    cargo test

# Test code formatting
test-fmt:
    cargo fmt --all -- --check

# Run the benchmark (runs test-integration first to ensure test data exists)
bench: test-integration
    cargo bench

# Run the integration test (requires osmium and uv)
test-integration:
    cd test && ./test.sh
