#!/usr/bin/env bash
# ABOUTME: Materializes pristine AgentFS plus the ordered AiryFS patch series.
# ABOUTME: Builds the local TypeScript SDK and the linux/amd64 Rust CLI.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
UPSTREAM_DIR="$ROOT_DIR/upstream"
PATCH_DIR="$ROOT_DIR/patches"
DEPENDENCY_PATCH_DIR="$ROOT_DIR/dependency-patches"
BUILD_DIR="$ROOT_DIR/.build"
TARGET_DIR="$ROOT_DIR/.target"
CONTAINER_DIR="$(cd "$ROOT_DIR/../container" && pwd)"

if [[ ! -f "$UPSTREAM_DIR/cli/Cargo.toml" || ! -f "$UPSTREAM_DIR/sdk/typescript/package.json" ]]; then
  echo "AgentFS upstream snapshot is incomplete: $UPSTREAM_DIR" >&2
  exit 1
fi

# .build is generated exclusively by this script. The exact-path guard keeps
# cleanup from ever escaping the repository's AgentFS directory.
if [[ "$BUILD_DIR" != "$ROOT_DIR/.build" ]]; then
  echo "Refusing to clean unexpected build directory: $BUILD_DIR" >&2
  exit 1
fi
rm -rf -- "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cp -R "$UPSTREAM_DIR/." "$BUILD_DIR/"

git -C "$BUILD_DIR" init -q
git -C "$BUILD_DIR" add -A
git -C "$BUILD_DIR" -c user.name=AiryFS -c user.email=airyfs@localhost commit -q -m "Pristine AgentFS upstream"

for patch in "$PATCH_DIR"/*.patch; do
  echo "Applying $(basename "$patch")"
  git -C "$BUILD_DIR" apply --check "$patch"
  git -C "$BUILD_DIR" apply --3way "$patch"
done

echo "Building and testing AgentFS TypeScript SDK..."
npm --prefix "$BUILD_DIR/sdk/typescript" ci
npm --prefix "$BUILD_DIR/sdk/typescript" run build
npm --prefix "$BUILD_DIR/sdk/typescript" test

DOCKER_CA_ARGS=()
CA_SETUP=""
if [[ -n "${DOCKER_CA_CERT:-}" ]]; then
  DOCKER_CA_ARGS=(-v "$DOCKER_CA_CERT:/usr/local/share/ca-certificates/custom-ca.crt:ro")
  CA_SETUP="update-ca-certificates >/dev/null 2>&1 &&"
fi

echo "Building and testing AgentFS Rust CLI and SDK..."
mkdir -p "$TARGET_DIR"
docker run --rm \
  --platform linux/amd64 \
  -v "$BUILD_DIR:/src" \
  -v "$TARGET_DIR:/target" \
  -v "$DEPENDENCY_PATCH_DIR:/dependency-patches:ro" \
  -v cargo-cache:/usr/local/cargo/registry \
  "${DOCKER_CA_ARGS[@]}" \
  -w /src \
  -e RUSTUP_TOOLCHAIN=1.88.0 \
  -e CARGO_BUILD_JOBS=2 \
  -e CARGO_TARGET_DIR=/target \
  rust:1.88-slim \
  bash -c "apt-get update -qq && apt-get install -y -qq pkg-config libssl-dev cmake build-essential liblzma-dev ca-certificates patch >/dev/null 2>&1 && $CA_SETUP cargo fetch --manifest-path sdk/rust/Cargo.toml && libsql_sources=(/usr/local/cargo/registry/src/*/libsql-0.9.30) && if [[ \${#libsql_sources[@]} -ne 1 ]]; then echo 'Expected exactly one libsql 0.9.30 source directory' >&2; exit 1; fi && if patch --batch --forward --dry-run -d \"\${libsql_sources[0]}\" -p1 < /dependency-patches/libsql-0.9.30-skip-remote-describe.patch >/dev/null 2>&1; then patch --batch --forward -d \"\${libsql_sources[0]}\" -p1 < /dependency-patches/libsql-0.9.30-skip-remote-describe.patch; elif ! patch --batch --reverse --dry-run -d \"\${libsql_sources[0]}\" -p1 < /dependency-patches/libsql-0.9.30-skip-remote-describe.patch >/dev/null 2>&1; then echo 'libsql 0.9.30 dependency patch does not apply cleanly' >&2; exit 1; fi && cargo test --manifest-path sdk/rust/Cargo.toml && cargo test --manifest-path cli/Cargo.toml --no-default-features && cargo build --manifest-path cli/Cargo.toml --release --no-default-features"

mkdir -p "$CONTAINER_DIR/bin"
cp "$TARGET_DIR/release/agentfs" "$CONTAINER_DIR/bin/agentfs"
echo "Built: $CONTAINER_DIR/bin/agentfs"
