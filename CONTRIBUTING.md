# Contributing

## Prerequisites

- A stable Rust toolchain (`rustup` recommended).
- **GMP** — required by the `rug` crate (arbitrary-precision integers used by
  the decimal and interval encodings):
  - macOS: `brew install gmp`
  - Debian/Ubuntu: `sudo apt install libgmp-dev`

## Build and test

The `Makefile` wraps the standard cargo commands:

```bash
make build      # cargo build
make test       # cargo test
make clippy     # cargo clippy --all-targets
make check-fmt  # cargo fmt --check
make fmt        # cargo fmt
make pr         # check-fmt + clippy + test
```

Run `make pr` before opening a pull request; it is the same gate CI applies.
