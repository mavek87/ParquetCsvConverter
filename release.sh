#!/usr/bin/env bash
# Usage:
#   ./release.sh              # bump patch (default)
#   ./release.sh patch        # bump patch
#   ./release.sh minor        # bump minor
#   ./release.sh major        # bump major
#   ./release.sh 2.0.0        # set exact version

set -euo pipefail

DOCKER_IMAGE="mavek87/parquet-csv"
PYPROJECT="pyproject.toml"

# ── helpers ──────────────────────────────────────────────────────────────────

die() { echo "Error: $*" >&2; exit 1; }

# Read current version from pyproject.toml
current_version() {
    grep -E '^version = ' "$PYPROJECT" | sed 's/version = "\(.*\)"/\1/'
}

# Split semver into parts
split_version() {
    local v="$1"
    IFS='.' read -r MAJOR MINOR PATCH <<< "$v"
}

bump_version() {
    local current="$1"
    local bump="${2:-patch}"

    split_version "$current"

    case "$bump" in
        major) echo "$((MAJOR + 1)).0.0" ;;
        minor) echo "${MAJOR}.$((MINOR + 1)).0" ;;
        patch) echo "${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
        # exact version passed (e.g. 2.0.0)
        [0-9]*.*.*) echo "$bump" ;;
        *) die "Unknown bump type '$bump'. Use major, minor, patch, or X.Y.Z." ;;
    esac
}

# ── main ─────────────────────────────────────────────────────────────────────

# Check Docker login before doing anything else
DOCKER_USER="$(docker system info 2>/dev/null | grep -E '^\s*Username:' | awk '{print $2}')"
if [[ -z "$DOCKER_USER" ]]; then
    die "Not logged in to Docker Hub. Run 'docker login' first."
fi
echo "Docker Hub user : $DOCKER_USER"

BUMP="${1:-patch}"

OLD_VERSION="$(current_version)"
[[ -z "$OLD_VERSION" ]] && die "Could not read version from $PYPROJECT"

NEW_VERSION="$(bump_version "$OLD_VERSION" "$BUMP")"

echo "Current version : $OLD_VERSION"
echo "New version     : $NEW_VERSION"
echo "Docker image    : $DOCKER_IMAGE"
echo ""
read -rp "Proceed? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

echo ""
echo "── 1/4  Bumping version in $PYPROJECT ─────────────────────────────────"
sed -i "s/^version = \"${OLD_VERSION}\"/version = \"${NEW_VERSION}\"/" "$PYPROJECT"
echo "Done: version = \"$NEW_VERSION\""

echo ""
echo "── 2/4  Building Docker image ──────────────────────────────────────────"
docker build \
    -t "${DOCKER_IMAGE}:latest" \
    -t "${DOCKER_IMAGE}:v${NEW_VERSION}" \
    .

echo ""
echo "── 3/4  Pushing tags to Docker Hub ─────────────────────────────────────"
docker push "${DOCKER_IMAGE}:latest"
docker push "${DOCKER_IMAGE}:v${NEW_VERSION}"

echo ""
echo "── 4/4  Done ───────────────────────────────────────────────────────────"
echo "Released ${DOCKER_IMAGE}:v${NEW_VERSION} (also tagged as latest)"
echo "https://hub.docker.com/r/${DOCKER_IMAGE}/tags"
