#!/usr/bin/env bash
# Publish a pathlockd release: validate, tag, push, and create the GitHub
# release from a notes file.
#
# Usage:
#   scripts/release.sh [--dry-run] [--prerelease] [--draft] [--build] [--docker] [--docker-force] [--arm64] <tag>
#
# <tag> is the git tag to create, e.g. v0.2.1. Release notes are read from
#   release_notes/<tag>/gh.md     (required; author it before releasing)
# and used both as the GitHub release body and the annotated-tag message.
#
# By default the script runs `cargo check --release --locked` to verify
# buildability without producing binaries. Pass --build to compile full release
# + debug binaries, package them, and attach them to the GitHub release.
#
# Preconditions (checked, fails fast before anything irreversible):
#   - cargo, gh (authenticated), git, awk installed;
#   - tar, sha256sum required only when --build is passed;
#   - docker required only when --docker is passed;
#   - clean working tree, not behind the remote;
#   - Cargo.toml's [package] version == <tag> without the leading 'v'
#     (if it doesn't match and --dry-run is NOT set, Cargo.toml + Cargo.lock
#     are bumped and committed automatically);
#   - existing local tag, remote tag, or GitHub release are skipped (idempotent).
#
# Docker images are published automatically by the GitHub Actions workflow on
# every v* tag push; use --docker only for local / manual image builds.
# The docker image is built with RUSTFLAGS="-C target-cpu=x86-64-v3" by default.
# Add --docker-force to rebuild and push even if the image tag already exists in GHCR.
# Add --arm64 to also build and push the linux/arm64 target alongside linux/amd64
# (skipped by default; requires the local builder to have arm64 emulation).
# Set GHCR_TOKEN to a PAT with write:packages to avoid interactive gh auth prompts.
#
# Artifacts (when --build):
#   dist/<tag>/pathlockd-<version>-linux-amd64.tar.gz        (release, stripped)
#   dist/<tag>/pathlockd-<version>-linux-amd64-debug.tar.gz  (debug, with symbols)
#   dist/<tag>/SHA256SUMS
set -euo pipefail

usage() {
  sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'
}

die()  { echo "✖ $*" >&2; exit 1; }
note() { echo "▶ $*"; }
warn() { echo "⚠ $*" >&2; }

# ---------------------------------------------------------------- args
DRY_RUN=0; PRERELEASE=0; DRAFT=0; BUILD=0; DOCKER=0; DOCKER_FORCE=0; ARM64=0; TAG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)       DRY_RUN=1 ;;
    --prerelease)    PRERELEASE=1 ;;
    --draft)         DRAFT=1 ;;
    --build)         BUILD=1 ;;
    --docker)        DOCKER=1 ;;
    --docker-force)  DOCKER=1; DOCKER_FORCE=1 ;;
    --arm64)         ARM64=1 ;;
    -h|--help)       usage; exit 0 ;;
    -*)              die "unknown flag: $1 (try --help)" ;;
    *)               [ -z "$TAG" ] || die "unexpected extra argument: $1"; TAG="$1" ;;
  esac
  shift
done
[ -n "$TAG" ] || { usage; exit 2; }

cd "$(dirname "$0")/.."

# ---------------------------------------------------------------- tooling
for t in cargo gh git awk; do
  command -v "$t" >/dev/null 2>&1 || die "required tool not found: $t"
done
if [ "$BUILD" = 1 ]; then
  for t in tar sha256sum; do
    command -v "$t" >/dev/null 2>&1 || die "required tool not found: $t (needed by --build)"
  done
fi
[ "$DOCKER" = 0 ] || command -v docker >/dev/null 2>&1 || die "required tool not found: docker (needed by --docker)"
gh auth status >/dev/null 2>&1 || die "gh is not authenticated (run: gh auth login)"

# ---------------------------------------------------------------- tag / version
case "$TAG" in
  v[0-9]*.[0-9]*.[0-9]*) VERSION="${TAG#v}" ;;
  *) die "tag must look like vMAJOR.MINOR.PATCH (e.g. v0.1.2), got: $TAG" ;;
esac

GH_USER="$(gh api user --jq .login)"
GHCR_IMAGE="ghcr.io/$GH_USER/pathlockd"

NOTES="release_notes/$TAG/gh.md"
[ -f "$NOTES" ] || die "release notes not found: $NOTES — create it first"
[ -s "$NOTES" ] || die "release notes are empty: $NOTES"

PKG_VERSION="$(awk -F'"' '/^\[/{p=($0=="[package]")} p&&/^version[[:space:]]*=/{print $2; exit}' Cargo.toml)"
[ -n "$PKG_VERSION" ] || die "could not read [package] version from Cargo.toml"
if [ "$PKG_VERSION" != "$VERSION" ]; then
  [ "$DRY_RUN" = 0 ] \
    || die "Cargo.toml version is $PKG_VERSION but tag is $TAG — bump it and commit first (or run without --dry-run to auto-bump)"
  note "bumping Cargo.toml $PKG_VERSION → $VERSION …"
  sed -i "s/^version = \"$PKG_VERSION\"/version = \"$VERSION\"/" Cargo.toml
  cargo update --package pathlockd
  git add Cargo.toml Cargo.lock
  git commit -m "chore: release $TAG"
fi

# ---------------------------------------------------------------- git state
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
[ "$BRANCH" = "main" ] || warn "releasing from branch '$BRANCH' (not main)"

LOCAL_TAG_EXISTS=0; REMOTE_TAG_EXISTS=0; RELEASE_EXISTS=0

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
  && LOCAL_TAG_EXISTS=1 && warn "local tag $TAG already exists — will skip tagging"
git fetch --quiet --tags origin 2>/dev/null || warn "git fetch failed (continuing)"
git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1 \
  && REMOTE_TAG_EXISTS=1 && warn "remote tag $TAG already exists — will skip tag push"
gh release view "$TAG" >/dev/null 2>&1 \
  && RELEASE_EXISTS=1 && warn "GitHub release $TAG already exists — will skip release creation"

# A clean tree is only required when we are about to create a new tag.
[ "$LOCAL_TAG_EXISTS" = 1 ] \
  || [ -z "$(git status --porcelain)" ] \
  || die "working tree is dirty — commit or stash before releasing"

if git rev-parse -q --verify "refs/remotes/origin/$BRANCH" >/dev/null; then
  git merge-base --is-ancestor "origin/$BRANCH" HEAD \
    || die "local $BRANCH is behind origin/$BRANCH — pull/rebase before releasing"
fi

# ---------------------------------------------------------------- check / build + package
DIST="dist/$TAG"
REL_TGZ="pathlockd-$VERSION-linux-amd64.tar.gz"
DBG_TGZ="pathlockd-$VERSION-linux-amd64-debug.tar.gz"

if [ "$BUILD" = 1 ]; then
  if [ -f "$DIST/$REL_TGZ" ] && [ -f "$DIST/$DBG_TGZ" ] && [ -f "$DIST/SHA256SUMS" ]; then
    note "dist artifacts already present in $DIST — skipping build and package."
  else
    note "building release + debug binaries (linux/amd64) …"
    cargo build --release --locked
    cargo build --locked
    REL="target/release/pathlockd"; DBG="target/debug/pathlockd"
    [ -x "$REL" ] && [ -x "$DBG" ] || die "expected binaries missing after build"
    GOT="$("$REL" --version | awk '{print $2}')"
    [ "$GOT" = "$VERSION" ] || die "release binary reports version $GOT, expected $VERSION"
    rm -rf "$DIST"; mkdir -p "$DIST/.stage"
    cp "$REL" "$DIST/.stage/pathlockd"; tar -C "$DIST/.stage" -czf "$DIST/$REL_TGZ" pathlockd
    cp "$DBG" "$DIST/.stage/pathlockd"; tar -C "$DIST/.stage" -czf "$DIST/$DBG_TGZ" pathlockd
    rm -rf "$DIST/.stage"
    ( cd "$DIST" && sha256sum "$REL_TGZ" "$DBG_TGZ" > SHA256SUMS )
  fi
  note "artifacts in $DIST:"
  ls -lh "$DIST"/*.tar.gz "$DIST"/SHA256SUMS | awk '{print "   " $9 "  (" $5 ")"}'
else
  note "checking buildability (pass --build to compile and package artifacts) …"
  cargo check --release --locked
  note "cargo check passed."
fi

# ---------------------------------------------------------------- docker build (opt-in)
if [ "$DOCKER" = 1 ]; then
  # Ensure a buildx builder capable of multi-platform builds exists.
  docker buildx inspect pathlockd-builder >/dev/null 2>&1 \
    || docker buildx create --name pathlockd-builder --use --bootstrap
  docker buildx use pathlockd-builder
else
  note "skipping docker build (pass --docker to build and push images locally)."
fi

# ---------------------------------------------------------------- dry-run stop
if [ "$DRY_RUN" = 1 ]; then
  note "dry-run complete — would tag $TAG on $BRANCH ($(git rev-parse --short HEAD)), push, and create the GitHub release."
  [ "$BUILD" = 1 ]  && note "  --build:  would attach artifacts from $DIST to the release."
  [ "$BUILD" = 0 ]  && note "  no --build: release will be created without binary artifacts."
  if [ "$DOCKER" = 1 ]; then
    PLATFORMS="linux/amd64"; [ "$ARM64" = 1 ] && PLATFORMS="linux/amd64,linux/arm64"
    note "  --docker:    would push $GHCR_IMAGE:$VERSION ($PLATFORMS, x86-64-v3 optimized) to GHCR."
    [ "$DOCKER_FORCE" = 1 ] && note "  --docker-force: will overwrite existing image tag."
  fi
  note "  The GitHub Actions workflow will publish the default container image automatically on tag push."
  exit 0
fi

# ---------------------------------------------------------------- publish (irreversible)
if [ "$LOCAL_TAG_EXISTS" = 0 ]; then
  note "tagging $TAG (annotated; message from $NOTES) …"
  git tag -a "$TAG" -F "$NOTES"
else
  note "local tag $TAG already exists — skipping."
fi

note "pushing $BRANCH …"
git push origin "$BRANCH"

if [ "$REMOTE_TAG_EXISTS" = 0 ]; then
  note "pushing tag $TAG …"
  git push origin "$TAG"
else
  note "remote tag $TAG already exists — skipping tag push."
fi

GH_FLAGS=(--title "$TAG" --notes-file "$NOTES" --verify-tag)
[ "$PRERELEASE" = 1 ] && GH_FLAGS+=(--prerelease)
[ "$DRAFT" = 1 ] && GH_FLAGS+=(--draft)
if [ "$RELEASE_EXISTS" = 0 ]; then
  note "creating GitHub release $TAG …"
  if [ "$BUILD" = 1 ]; then
    gh release create "$TAG" "${GH_FLAGS[@]}" \
      "$DIST/$REL_TGZ" "$DIST/$DBG_TGZ" "$DIST/SHA256SUMS"
  else
    gh release create "$TAG" "${GH_FLAGS[@]}"
  fi
else
  note "GitHub release $TAG already exists — skipping."
fi

if [ "$DOCKER" = 1 ]; then
  # Use GHCR_TOKEN env var (PAT with write:packages) if set; otherwise fall back
  # to the existing gh session token (requires write:packages scope already granted).
  GHCR_LOGIN_TOKEN="${GHCR_TOKEN:-$(gh auth token)}"
  note "logging in to ghcr.io as $GH_USER …"
  printf '%s' "$GHCR_LOGIN_TOKEN" | docker login ghcr.io -u "$GH_USER" --password-stdin

  DOCKER_PLATFORMS="linux/amd64"; [ "$ARM64" = 1 ] && DOCKER_PLATFORMS="linux/amd64,linux/arm64"
  if [ "$DOCKER_FORCE" = 0 ] && docker buildx imagetools inspect "$GHCR_IMAGE:$VERSION" >/dev/null 2>&1; then
    note "$GHCR_IMAGE:$VERSION already exists — skipping multi-platform push (use --docker-force to override)."
  else
    note "pushing $GHCR_IMAGE:$VERSION ($DOCKER_PLATFORMS, x86-64-v3 optimized) …"
    docker buildx build \
      --platform "$DOCKER_PLATFORMS" \
      --build-arg RUSTFLAGS="-C target-cpu=x86-64-v3" \
      -t "$GHCR_IMAGE:$VERSION" \
      --push \
      .
  fi
fi

note "done:"
gh release view "$TAG" --json url,assets --jq '.url, (.assets[] | "  asset: " + .name)'
note "the GitHub Actions workflow will publish container images to GHCR automatically."
