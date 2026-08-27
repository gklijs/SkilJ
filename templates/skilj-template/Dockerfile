# syntax=docker/dockerfile:1

# Multi-stage build producing a genuinely `FROM scratch` final image - no
# OS, no shell, no package manager, nothing beyond the statically-linked
# `server` binary and a CA bundle. See this crate's own README.md for the
# `docker build`/`docker run` invocation.

FROM rust:1-alpine AS builder
# Alpine's own musl toolchain builds `ring`'s C/asm shims natively during
# `cargo build` - no cross-compiler needed, unlike cross-compiling the
# musl target from a glibc-based image (which would need a separate
# `x86_64-linux-musl-gcc`).
RUN apk add --no-cache musl-dev openssh-client git ca-certificates \
    && mkdir -p -m 0700 ~/.ssh && ssh-keyscan codeberg.org >> ~/.ssh/known_hosts
# `skilj`/`skilj-core` aren't on crates.io yet (see this crate's own
# Cargo.toml and README.md) - this build needs the host's ssh-agent
# forwarded in (`docker build --ssh default ...`, not a key baked into a
# layer) to fetch the private git dependency.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
WORKDIR /app
COPY . .
RUN --mount=type=ssh cargo build --release --target x86_64-unknown-linux-musl

# `x86_64-unknown-linux-musl` statically links by default (the target's
# `crt-static` default) - that's the actual precondition for `scratch`
# working at all. Verify with `ldd target/x86_64-unknown-linux-musl/release/server`
# if in doubt - it should say "not a dynamic executable".
FROM scratch AS runtime
# scratch has nothing - not even a CA bundle or /etc/passwd. Docker
# itself injects /etc/resolv.conf at container-start, so DNS needs no
# help, but TLS verification (JWKS fetches, a TLS Postgres connection)
# needs a real CA bundle copied in explicitly.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/server /server
EXPOSE 8080
# Numeric UID/GID (the conventional "nobody") rather than a named user -
# scratch has no /etc/passwd for a name to resolve against.
USER 65534:65534
ENTRYPOINT ["/server"]
