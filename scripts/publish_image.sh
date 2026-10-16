#!/usr/bin/env bash
# Build and push the xLB Docker image to GHCR and/or Docker Hub.
#
# Usage:
#   # 1. Authenticate once:
#   #    echo "$GHCR_TOKEN" | docker login ghcr.io -u eeroeternal --password-stdin
#   #    echo "$DOCKERHUB_TOKEN" | docker login -u eeroeternal --password-stdin
#   # 2. Build & push current Cargo.toml version + :latest to GHCR and Docker Hub:
#   bash scripts/publish_image.sh
#   # 3. Multi-arch build & push (linux/amd64,linux/arm64):
#   PLATFORMS=linux/amd64,linux/arm64 bash scripts/publish_image.sh
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${REGISTRY_IMAGE:-ghcr.io/eeroeternal/xlb}"
DOCKERHUB_IMAGE="${DOCKERHUB_IMAGE:-eeroeternal/xlb}"
VERSION="${VERSION:-$(awk -F'"' '/^version = / {print $2; exit}' Cargo.toml)}"
PLATFORMS="${PLATFORMS:-}"

if [[ -z "$VERSION" ]]; then
  echo "error: failed to read version from Cargo.toml" >&2
  exit 1
fi

TAGS=(-t "xlb:${VERSION}" -t "xlb:latest" -t "${IMAGE}:${VERSION}" -t "${IMAGE}:latest")
if [[ -n "$DOCKERHUB_IMAGE" ]]; then
  TAGS+=(-t "${DOCKERHUB_IMAGE}:${VERSION}" -t "${DOCKERHUB_IMAGE}:latest")
fi

echo "==> Publishing ${IMAGE}:${VERSION} and ${IMAGE}:latest${DOCKERHUB_IMAGE:+ (plus ${DOCKERHUB_IMAGE}:${VERSION} and ${DOCKERHUB_IMAGE}:latest)}"

if [[ -n "$PLATFORMS" ]]; then
  docker buildx create --name xlb-builder --use >/dev/null 2>&1 || docker buildx use xlb-builder
  docker buildx build \
    --platform "$PLATFORMS" \
    "${TAGS[@]}" \
    --push \
    .
else
  docker build \
    "${TAGS[@]}" \
    .
  docker push "${IMAGE}:${VERSION}"
  docker push "${IMAGE}:latest"
  if [[ -n "$DOCKERHUB_IMAGE" ]]; then
    docker push "${DOCKERHUB_IMAGE}:${VERSION}"
    docker push "${DOCKERHUB_IMAGE}:latest"
  fi
fi

echo "✓ Published ${IMAGE}:${VERSION} and ${IMAGE}:latest${DOCKERHUB_IMAGE:+ (and ${DOCKERHUB_IMAGE}:${VERSION}, ${DOCKERHUB_IMAGE}:latest)}"
