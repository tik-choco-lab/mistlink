set shell := ["bash", "-lc"]

# Homebrew include path for cgo (<opus/opus.h>); harmless where it doesn't exist
export CGO_CFLAGS := env_var_or_default("CGO_CFLAGS", "-I/opt/homebrew/include")

default:
    @just --list

# Build the mistlink binary
build:
    go build -o bin/mistlink ./cmd/mistlink

# Build with optimizations for release
build-release:
    go build -trimpath -ldflags "-s -w" -o bin/mistlink ./cmd/mistlink

# Run tests
test:
    go test ./...

# Vendor mistlib (mistlib-core + mistlib-native) per .env MISTLIB_REPO/MISTLIB_REF
vendor-mistlib:
    sh scripts/vendor-mistlib.sh

# Build the vendored mistlib (native, release)
build-mistlib:
    cargo build --release -p mistlib-native --manifest-path third_party/mistlib/Cargo.toml

# Build mistlink.exe for Windows via Docker (bundles Opus/FDK-AAC statically)
build-windows:
    docker build -t mistlink-builder -f Dockerfile.build .
    docker create --name mistlink-temp mistlink-builder
    docker cp mistlink-temp:/app/mistlink.exe ./mistlink.exe
    docker rm mistlink-temp

# Remove build artifacts
clean:
    rm -rf bin mistlink.exe
