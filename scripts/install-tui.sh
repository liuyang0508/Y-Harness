#!/bin/sh
set -eu

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required; install Rust 1.88 or newer from https://rustup.rs" >&2
  exit 1
fi

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec cargo install --locked --force --path "$repo_root/clients/tui" "$@"
