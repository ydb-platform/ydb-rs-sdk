#!/bin/bash

set -eux

CRATE_NAME="$1"
VERSION_PART="$2"
GIT_EMAIL="$3"

declare -a GIT_TAGS
declare -a CRATES

function git_set_tags(){
  git config user.name "robot"
  git config user.email "$GIT_EMAIL"

  git commit -am "bump version for $CRATE_NAME, $VERSION_PART"

  local GIT_TAG
  for GIT_TAG in "${GIT_TAGS[@]}";  do
    git tag -f "$GIT_TAG"
  done
}

function git_push_tags() {
  local GIT_TAG remote_sha local_sha
  for GIT_TAG in "${GIT_TAGS[@]}"; do
    remote_sha="$(git ls-remote --tags origin "refs/tags/$GIT_TAG" | cut -f1)"
    local_sha="$(git rev-parse "$GIT_TAG")"
    if [ -n "$remote_sha" ]; then
      if [ "$remote_sha" = "$local_sha" ]; then
        echo "Tag $GIT_TAG already exists on remote at the same commit, skipping push"
      else
        echo "Tag $GIT_TAG exists on remote at a different commit, force-updating"
        git push origin "refs/tags/$GIT_TAG":refs/tags/"$GIT_TAG" --force
      fi
    else
      git push origin "$GIT_TAG"
    fi
  done
}

function publish_crate() {
    local CRATE_NAME="$1"
    local publish_output
    if publish_output="$(cd "$CRATE_NAME" && cargo publish 2>&1)"; then
        echo "$publish_output"
        return 0
    fi
    echo "$publish_output"
    if echo "$publish_output" | grep -q 'already exists on crates.io'; then
        echo "$CRATE_NAME is already on crates.io, continuing"
        return 0
    fi
    return 1
}

function crate_published_on_crates_io() {
    local CRATE_NAME="$1"
    local VERSION="$2"
    cargo info --registry crates-io "${CRATE_NAME}@${VERSION}" >/dev/null 2>&1
}

function publish_ydb_dependency_crates() {
    local GRPC_VERSION
    GRPC_VERSION=$(version_get "ydb-grpc")
    if crate_published_on_crates_io "ydb-grpc" "$GRPC_VERSION"; then
        echo "ydb-grpc $GRPC_VERSION is already on crates.io"
        return
    fi
    echo "Publishing ydb-grpc $GRPC_VERSION before ydb (crates.io tarball drops path= deps)"
    publish_crate "ydb-grpc"
}

function version_get() {
  local CRATE_NAME="$1"
  local VERSION_LINE VERSION

  VERSION_LINE="$(grep "^version\\s*=" "$CRATE_NAME/Cargo.toml")"
  VERSION=$(echo "$VERSION_LINE" | cut -d '"' -f 2)
  echo "$VERSION"
}

function version_increment()
{
  local VERSION UP_PART VERSION_MAJOR VERSION_MINOR VERSION_PATCH

  VERSION="$1"
  UP_PART="$2"
  VERSION_MAJOR=$(echo "$VERSION" | cut -d '.' -f 1)
  VERSION_MINOR=$(echo "$VERSION" | cut -d '.' -f 2)
  VERSION_PATCH=$(echo "$VERSION" | cut -d '.' -f 3)

  case "$UP_PART" in
    major)
      VERSION_MAJOR=$((VERSION_MAJOR+1))
      VERSION_MINOR=0
      VERSION_MINOR=0
      ;;
    minor)
      VERSION_MINOR=$((VERSION_MINOR+1))
      VERSION_PATCH=0
      ;;
    patch)
      VERSION_PATCH=$((VERSION_PATCH+1))
  esac

  echo "$VERSION_MAJOR.$VERSION_MINOR.$VERSION_PATCH"
}

function version_set() {
  local CRATE_NAME="$1"
  local VERSION="$2"

  sed -i.bak -e "s/^version *=.*/version = \"$VERSION\"/" "$CRATE_NAME/Cargo.toml"
  if [[ "$CRATE_NAME" == "ydb" ]]; then
    sed -i -e "s/^ydb *=.*/ydb = \"$VERSION\"/" "README.md"
  fi
}

function version_dep_set() {
  local DEP_NAME="$1"
  local VERSION="$2"

  for FILE in $(find . -mindepth 2 -maxdepth 2 -name Cargo.toml); do
    sed -i.bak -e "s|^$DEP_NAME *=.*|$DEP_NAME = \\{ version = \"$VERSION\", path=\"../$DEP_NAME\"\\}|" "$FILE"
  done
}

function bump_version() {
  local CRATE_NAME="$1"
  local VERSION_PART="$2"

  local VERSION
  VERSION=$(version_get "$CRATE_NAME")
  VERSION=$(version_increment "$VERSION" "$VERSION_PART")
  version_set "$CRATE_NAME" "$VERSION"
  GIT_TAGS+=("$CRATE_NAME-$VERSION")
  CRATES+=("$CRATE_NAME")

  mkdir -p .crate-versions
  echo "$VERSION" > ".crate-versions/$CRATE_NAME"

  case "$CRATE_NAME" in
    ydb)
      version_dep_set "ydb" "$VERSION"
      ;;
    ydb-grpc)
      version_dep_set "ydb-grpc" "$VERSION"
      ;;
    ydb-grpc-helpers)
      # Deprecated crate; not a workspace dependency of ydb / ydb-grpc.
      ;;
    *)
      echo "Unexpected crate name '$CRATE_NAME'"
      exit 1
  esac
}

bump_version "$CRATE_NAME" "$VERSION_PART"

# actualize Cargo.lock without compiling: only updates lock entries for
# workspace members whose versions just changed; external deps stay pinned.
cargo update --workspace

git diff

git_set_tags

# push tags before publish - for fix repository state if failed in middle of publish crates
git_push_tags

if [[ "$CRATE_NAME" == "ydb" ]]; then
  publish_ydb_dependency_crates
fi

for CRATE in "${CRATES[@]}"; do
  publish_crate "$CRATE"
done

# git push after publish crate - for run CI build check after all changed crates will published in crates repo
git push
