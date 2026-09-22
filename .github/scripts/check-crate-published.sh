#!/usr/bin/env bash
# Checks whether a crate's current Cargo.toml version is already published on
# crates.io, and sets the `already-published` step output accordingly.
#
# Usage: check-crate-published.sh <crate-name> <manifest-path> [release-tag]
#
# If release-tag is given, it must match the crate's Cargo.toml version (a
# leading "v" is stripped before comparing) regardless of whether that
# version is already published. This still needs to run and fail loudly even
# when the version is already published: skipping straight to "already
# published, nothing to do" would silently no-op a release cut without
# bumping this crate's version, instead of failing the way it used to before
# the already-published check below existed.
set -euo pipefail

crate_name="$1"
manifest_path="$2"
release_tag="${3:-}"

version=$(cargo metadata --no-deps --format-version=1 --manifest-path "$manifest_path" \
  | jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version')

if [ -z "$version" ]; then
  echo "::error::Could not read a version for crate '$crate_name' from $manifest_path" >&2
  exit 1
fi

if [ -n "$release_tag" ]; then
  tag_version="${release_tag#v}"
  if [ "$tag_version" != "$version" ]; then
    echo "::error::Release tag '$release_tag' does not match $crate_name's Cargo.toml version '$version'" >&2
    exit 1
  fi
fi

status=$(curl -sS -o /dev/null -w '%{http_code}' \
  -A "osm-pbf-parquet-ci (+https://github.com/OvertureMaps/osm-pbf-parquet)" \
  "https://crates.io/api/v1/crates/${crate_name}/${version}")

case "$status" in
  200)
    echo "already-published=true" >> "$GITHUB_OUTPUT"
    echo "::notice::${crate_name} ${version} is already published on crates.io; skipping publish."
    echo "${crate_name} \`${version}\` is already published, skipped." >> "$GITHUB_STEP_SUMMARY"
    ;;
  404)
    echo "already-published=false" >> "$GITHUB_OUTPUT"
    ;;
  *)
    echo "::error::Unexpected crates.io API response ($status) checking ${crate_name} ${version}" >&2
    exit 1
    ;;
esac
