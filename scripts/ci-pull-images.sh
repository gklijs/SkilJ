#!/usr/bin/env bash
# Pulls the images the container tests start, before they run, so that
# Docker Hub's limit on unauthenticated pulls doesn't turn them into skips
# (which scripts/ci-test.sh then fails the build for, as it must).
#
# testcontainers pulls an image only when creating its container answers
# 404, so an image pulled here is the one a test uses. Each comes from
# mirror.gcr.io, Google's public mirror of Docker Hub, which needs no
# account; an image it doesn't have comes from Docker Hub, retried with
# backoff. Either way it is tagged under the Docker Hub name the test asks
# for.
#
# A pull that fails here only warns: the test then tries itself, and if it
# can't start its container it skips, which fails the build with the
# test's own message. So a missing or stale entry below costs only the
# protection, never a run. scripts/ci.test.mjs checks that every image a
# test names with GenericImage::new is listed.
#
# The CI test job runs in a container without the docker CLI, so this
# speaks the Engine API over the mounted socket with curl.
#
# Usage: scripts/ci-pull-images.sh
set -uo pipefail

IMAGES=(
    # skilj-blob-s3/tests/store.rs
    adobe/s3mock:4.11.0
    # skilj-amqp/tests/amqp_bridge.rs
    apache/artemis:latest-alpine
    # skilj/tests/vault_transit_container.rs
    openbao/openbao:2.1.0
    # The defaults of testcontainers-modules 0.15's images: Nats
    # (skilj-nats), apache::Kafka (skilj-kafka), HashicorpVault (skilj).
    nats:2.10.14
    apache/kafka-native:3.8.0
    hashicorp/vault:1.17
)

socket="${DOCKER_HOST:-unix:///var/run/docker.sock}"
socket="${socket#unix://}"
if [[ ! -S "$socket" ]]; then
    echo "ci-pull-images: no Docker socket at $socket; the tests will pull for themselves" >&2
    exit 0
fi

api() {
    curl -sS --fail-with-body --unix-socket "$socket" -X POST "http://localhost/v1.41$1"
}

# POST /images/create answers 200 and reports a failed pull in its stream.
pull() {
    local from="$1" tag="$2" out
    out="$(api "/images/create?fromImage=${from}&tag=${tag}" 2>&1)" || { echo "$out" | tail -n 1; return 1; }
    if grep -q '"error"' <<<"$out"; then
        grep -o '"error":"[^"]*"' <<<"$out" | tail -n 1
        return 1
    fi
}

missing=0
for image in "${IMAGES[@]}"; do
    name="${image%:*}"
    tag="${image##*:}"
    # Official images live under library/ on a registry.
    [[ "$name" == */* ]] && path="$name" || path="library/$name"
    if pull "mirror.gcr.io/$path" "$tag" >/dev/null &&
        api "/images/mirror.gcr.io/$path:$tag/tag?repo=$name&tag=$tag" >/dev/null; then
        echo "ci-pull-images: $image from mirror.gcr.io"
        continue
    fi
    pulled=false
    for delay in 0 5 15; do
        sleep "$delay"
        if error="$(pull "$name" "$tag")"; then
            pulled=true
            break
        fi
        echo "ci-pull-images: $image from Docker Hub: ${error:-failed}" >&2
    done
    if $pulled; then
        echo "ci-pull-images: $image from Docker Hub"
    else
        echo "ci-pull-images: WARNING: couldn't pull $image; its test will try itself" >&2
        missing=$((missing + 1))
    fi
done
echo "ci-pull-images: ${#IMAGES[@]} images, $missing not pulled"
exit 0
