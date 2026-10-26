# syntax=docker/dockerfile:1

# Multi-stage build producing a genuinely `FROM scratch` final image - no
# OS, no shell, no package manager, nothing beyond the statically-linked
# `server` binary and a CA bundle. See this crate's own README.md for the
# `docker build`/`docker run` invocation.

FROM rust:1-alpine AS builder
# Alpine's own musl toolchain builds the C/asm shims of the crypto crates
# (`ring`, `aws-lc-rs` via sqlx and jsonwebtoken) natively during
# `cargo build` - no cross-compiler needed, unlike cross-compiling the
# musl target from a glibc-based image (which would need a separate
# `x86_64-linux-musl-gcc`).
RUN apk add --no-cache musl-dev ca-certificates
WORKDIR /app
COPY . .
# Without the embedded-Postgres `local` feature: a container is given a
# DATABASE_URL, and the image has nothing to run a Postgres with anyway.
RUN cargo build --release --no-default-features

# No `--target`: on Alpine the host target already is
# `<arch>-unknown-linux-musl`, so the same file builds for amd64 and arm64
# (and under `docker buildx --platform`). That target statically links by
# default (its `crt-static` default) - the actual precondition for
# `scratch` working at all. Verify with `ldd target/release/server` if in
# doubt - it should say "not a dynamic executable".
FROM scratch AS runtime
# scratch has nothing - not even a CA bundle or /etc/passwd. Docker
# itself injects /etc/resolv.conf at container-start, so DNS needs no
# help, but TLS verification (JWKS fetches, a TLS Postgres connection)
# needs a real CA bundle copied in explicitly.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/release/server /server
EXPOSE 8080
# Numeric UID/GID (the conventional "nobody") rather than a named user -
# scratch has no /etc/passwd for a name to resolve against.
USER 65534:65534
ENTRYPOINT ["/server"]
