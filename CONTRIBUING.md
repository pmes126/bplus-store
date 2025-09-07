# Contributing

## Toolchain
- Rust **1.77+**
- `cargo`, `rustfmt`, `clippy`

## Dev loop
```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --all-features
