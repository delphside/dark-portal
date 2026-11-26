# syntax=docker/dockerfile:1
#
# The one image `dp` ships in (docs/5.1.3 *Technology decisions*): the same
# image in every environment, configured by that environment's `.env`
# (docs/5.4.1). It builds on both architectures `dp` ships for, x86-64 and
# ARM64, with `docker buildx build --platform`.
#
# Base images are pinned by tag. Pin them by digest too once the release
# process can read one: it is a seam of #11 (docs/5.1.3 *The image*).

# The builder's compiler is the one `rust-toolchain.toml` pins, so the image
# and CI build with the same one. Debian 12 is the builder's and the runtime's
# alike, so the binary's glibc is the runtime's.
FROM rust:1.100.0-slim-bookworm AS builder

# The bundled DuckDB compiles C++ and its build script wants `cc`/`c++`.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY src ./src
COPY sql ./sql
COPY examples ./examples

# The binary, and the step that installs the DuckDB extensions with the same
# DuckDB crate, into the home the runtime user will have. `dp` itself never
# installs one: autoinstall stays off.
ENV HOME=/home/dp
RUN cargo build --release --locked --bin dp --example install_extensions \
    && ./target/release/examples/install_extensions \
    && chmod -R a+rX /home/dp/.duckdb

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /home/dp --no-create-home dp

# The SQL `dp` runs is compiled into the binary (`include_str!`), so the
# binary is all of the program. The extensions are DuckDB's default
# extension folder under `HOME`, which is where it reads them.
COPY --from=builder /build/target/release/dp /usr/local/bin/dp
COPY --from=builder /home/dp/.duckdb /home/dp/.duckdb

ENV HOME=/home/dp
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/dp"]
