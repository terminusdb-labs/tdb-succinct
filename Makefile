.PHONY: help build test clippy check fmt check-fmt clean pr

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the library
	cargo build

test: ## Run all unit tests
	cargo test

clippy: ## Run clippy on all targets
	cargo clippy --all-targets

check: ## Run cargo check
	cargo check

fmt: ## Format all Rust code
	cargo fmt

check-fmt: ## Check formatting without changing files
	cargo fmt --check

clean: ## Remove build artifacts
	cargo clean

pr: check-fmt clippy test ## Run pre-PR checks (fmt, clippy, tests)
