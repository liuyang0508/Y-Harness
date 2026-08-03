#!/bin/sh
set -eu

usage() {
  echo "usage: $0 v<package-version>" >&2
  exit 64
}

if [ "$#" -ne 1 ]; then
  usage
fi

release_tag=$1
case "$release_tag" in
  v[0-9]*) ;;
  *)
    echo "release tag must use the v<package-version> form" >&2
    exit 65
    ;;
esac

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "release verification requires a Git worktree" >&2
  exit 66
fi

worktree_status=$(git status --porcelain=v1 --untracked-files=all)
if [ -n "$worktree_status" ]; then
  echo "release verification requires a clean worktree" >&2
  echo "$worktree_status" >&2
  exit 67
fi

head_commit=$(git rev-parse --verify HEAD)
tag_commit=$(git rev-parse --verify "${release_tag}^{commit}" 2>/dev/null || true)
if [ -z "$tag_commit" ]; then
  echo "release tag does not resolve to a commit: $release_tag" >&2
  exit 68
fi
if [ "$tag_commit" != "$head_commit" ]; then
  echo "release tag $release_tag does not point at HEAD $head_commit" >&2
  exit 69
fi

package_version() {
  package_id=$(cargo pkgid --manifest-path Cargo.toml -p "$1")
  case "$package_id" in
    *@*) printf '%s\n' "${package_id##*@}" ;;
    *#*) printf '%s\n' "${package_id##*#}" ;;
    *)
      echo "cannot parse Cargo package identity: $package_id" >&2
      exit 70
      ;;
  esac
}

engine_version=$(package_version y-harness)
tui_version=$(package_version y-harness-tui)
expected_tag="v${engine_version}"

if [ "$release_tag" != "$expected_tag" ]; then
  echo "release tag $release_tag does not match y-harness $engine_version" >&2
  exit 71
fi
if [ "$tui_version" != "$engine_version" ]; then
  echo "yh-tui $tui_version does not match y-harness $engine_version" >&2
  exit 72
fi

notes_path="docs/release-notes-v${engine_version}.md"
if [ ! -f "$notes_path" ]; then
  echo "release notes are missing: $notes_path" >&2
  exit 73
fi
if ! grep -F "v${engine_version}" "$notes_path" >/dev/null 2>&1; then
  echo "release notes do not name v${engine_version}: $notes_path" >&2
  exit 74
fi

for required_path in Cargo.lock README.md LICENSE-MIT LICENSE-APACHE; do
  if [ ! -f "$required_path" ]; then
    echo "required release input is missing: $required_path" >&2
    exit 75
  fi
done

git diff --check
cargo metadata --locked --format-version 1 --no-deps >/dev/null

printf 'release coordinate verified: tag=%s commit=%s engine=%s tui=%s notes=%s\n' \
  "$release_tag" "$head_commit" "$engine_version" "$tui_version" "$notes_path"
