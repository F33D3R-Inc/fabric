# syntax=docker/dockerfile:1
#
# fabricd — the Fabric control plane, with FacetQL's front door on its data
# port.
#
#   docker build -t fabricd .
#   docker run --rm -p 7710:7710 -p 7711:7711 \
#     -e FABRIC_ADMIN_TOKEN=... \
#     -v $PWD/deploy/fabric.json:/etc/fabric/fabric.json:ro \
#     fabricd
#
# Two things about this image are deliberate.
#
# **It runs as a non-root user with no shell.** The runtime layer is
# distroless: no package manager, no `sh`, nothing to pivot to. A control plane
# holds a credential for every database instance in the fleet, so the blast
# radius of a compromise here is the whole fleet, and the answer is to leave
# nothing in the container to compromise.
#
# **It has no HEALTHCHECK.** A `HEALTHCHECK` needs a program in the image to
# run, and adding curl to a distroless runtime for it would be trading the
# property above for a convenience. `GET /healthz` on the admin port is
# unauthenticated and answers exactly this question — point an orchestrator's
# HTTP probe (Kubernetes `httpGet`, an external checker) at it.

FROM rust:1-slim-bookworm AS build

WORKDIR /src

# The whole workspace: fabricd is the assembler and depends on every crate in
# it, so there is no smaller correct build context.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked --package fabric-daemon --bin fabricd

# distroless/cc: glibc and CA certificates, no shell and no package manager.
# `cc` rather than `static` because the binary is dynamically linked against
# the builder's libc; `:nonroot` runs as uid 65532.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/target/release/fabricd /usr/local/bin/fabricd

# The client-facing port speaks FacetQL's wire protocol: point
# FACET_DATABASE_URL here instead of at one FacetQL and nothing above changes.
EXPOSE 7710

# The operator port. Authenticated, and bound to loopback unless the
# configuration says otherwise — publish it deliberately or not at all.
EXPOSE 7711

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/fabricd"]
CMD ["--config", "/etc/fabric/fabric.json"]
