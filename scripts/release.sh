#!/usr/bin/env bash
# Publish a pathlockd release: validate, build linux/amd64 artifacts (release +
# debug), tag, push, and create the GitHub release from a notes file — the same
# steps done by hand for v0.1.1.
#
# Usage:
#   scripts/release.sh [--dry-run] [--prerelease] [--draft] [--docker] <tag>
#
# <tag> is the git tag to create, e.g. v0.1.2. Release notes are read from
#   release_notes/<tag>/gh.md     (required; author it before releasing)
# and used both as the GitHub release body and the annotated-tag message.
#
# Preconditions (checked, fails fast before anything irreversible):
#   - cargo, gh (authenticated), tar, sha256sum, git installed;
#   - docker required only when --docker is passed;
#   - clean working tree, not behind the remote;
#   - Cargo.toml's [package] version == <tag> without the leading 'v'
#     (if it doesn't match and --dry-run is NOT set, Cargo.toml + Cargo.lock
#     are bumped and committed automatically);
#   - the tag (local + remote) and a GitHub release for it do not yet exist.
#
# Docker images are published automatically by the GitHub Actions workflow on
# every v* tag push; use --docker only for local / manual image builds.
#
# Artifacts are built ON THIS HOST, so they are dynamically linked against the
# host's glibc/libssl3. For maximally portable binaries, build in the Dockerfile
# builder stage instead and attach those.
#
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
DRY_RUN=0; PRERELEASE=0; DRAFT=0; DOCKER=0; TAG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)    DRY_RUN=1 ;;
    --prerelease) PRERELEASE=1 ;;
    --draft)      DRAFT=1 ;;
    --docker)     DOCKER=1 ;;
    -h|--help)    usage; exit 0 ;;
    -*)           die "unknown flag: $1 (try --help)" ;;
    *)            [ -z "$TAG" ] || die "unexpected extra argument: $1"; TAG="$1" ;;
  esac
  shift
done
[ -n "$TAG" ] || { usage; exit 2; }

cd "$(dirname "$0")/.."

# ---------------------------------------------------------------- tooling
for t in cargo gh tar sha256sum git awk; do
  command -v "$t" >/dev/null 2>&1 || die "required tool not found: $t"
done
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
[ -z "$(git status --porcelain)" ] || die "working tree is dirty — commit or stash before releasing"

exists_abort() { # message
  if [ "$DRY_RUN" = 1 ]; then warn "$1 (dry-run: continuing)"; else die "$1"; fi
}
git rev-parse -q --verify "refs/tags/$TAG" >/dev/null && exists_abort "local tag $TAG already exists"
git fetch --quiet --tags origin 2>/dev/null || warn "git fetch failed (continuing)"
git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1 \
  && exists_abort "remote tag $TAG already exists"
gh release view "$TAG" >/dev/null 2>&1 && exists_abort "GitHub release $TAG already exists"

if git rev-parse -q --verify "refs/remotes/origin/$BRANCH" >/dev/null; then
  git merge-base --is-ancestor "origin/$BRANCH" HEAD \
    || die "local $BRANCH is behind origin/$BRANCH — pull/rebase before releasing"
fi

# ---------------------------------------------------------------- build + package
DIST="dist/$TAG"
REL_TGZ="pathlockd-$VERSION-linux-amd64.tar.gz"
DBG_TGZ="pathlockd-$VERSION-linux-amd64-debug.tar.gz"

if [ -f "$DIST/$REL_TGZ" ] && [ -f "$DIST/$DBG_TGZ" ] && [ -f "$DIST/SHA256SUMS" ]; then
  note "dist artifacts already present in $DIST — skipping build and package."
else
  note "building release + debug binaries (linux/amd64) …"
  cargo build --release
  cargo build
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

# ---------------------------------------------------------------- docker build (opt-in)
if [ "$DOCKER" = 1 ]; then
  if docker image inspect "$GHCR_IMAGE:$VERSION" >/dev/null 2>&1; then
    note "container image $GHCR_IMAGE:$VERSION already present locally — skipping build."
  else
    note "building container image ($GHCR_IMAGE:$VERSION) …"
    docker build -t "$GHCR_IMAGE:$VERSION" .
    note "building x86-64-v4 container image ($GHCR_IMAGE:$VERSION-x86-64-v4) …"
    docker build --build-arg RUSTFLAGS="-C target-cpu=x86-64-v4" \
      -t "$GHCR_IMAGE:$VERSION-x86-64-v4" .
  fi
else
  note "skipping docker build (pass --docker to build and push images locally)."
fi

# ---------------------------------------------------------------- dry-run stop
if [ "$DRY_RUN" = 1 ]; then
  note "dry-run complete — would tag $TAG on $BRANCH ($(git rev-parse --short HEAD)), push, and create the GitHub release with the assets above."
  [ "$DOCKER" = 1 ] && note "  --docker: would also push $GHCR_IMAGE:$VERSION and :$VERSION-x86-64-v4 to GHCR."
  note "  The GitHub Actions workflow will publish both container images automatically on tag push."
  exit 0
fi

# ---------------------------------------------------------------- publish (irreversible)
note "tagging $TAG (annotated; message from $NOTES) …"
git tag -a "$TAG" -F "$NOTES"
note "pushing $BRANCH and tag $TAG …"
git push origin "$BRANCH"
git push origin "$TAG"

GH_FLAGS=(--title "$TAG" --notes-file "$NOTES" --verify-tag)
[ "$PRERELEASE" = 1 ] && GH_FLAGS+=(--prerelease)
[ "$DRAFT" = 1 ] && GH_FLAGS+=(--draft)
note "creating GitHub release $TAG …"
gh release create "$TAG" "${GH_FLAGS[@]}" \
  "$DIST/$REL_TGZ" "$DIST/$DBG_TGZ" "$DIST/SHA256SUMS"

if [ "$DOCKER" = 1 ]; then
  note "logging in to ghcr.io as $GH_USER …"
  gh auth refresh -s write:packages
  gh auth token | docker login ghcr.io -u "$GH_USER" --password-stdin
  note "pushing $GHCR_IMAGE:$VERSION and :$VERSION-x86-64-v4 …"
  docker push "$GHCR_IMAGE:$VERSION"
  docker push "$GHCR_IMAGE:$VERSION-x86-64-v4"
fi

note "done:"
gh release view "$TAG" --json url,assets --jq '.url, (.assets[] | "  asset: " + .name)'
note "the GitHub Actions workflow will publish container images to GHCR automatically."
