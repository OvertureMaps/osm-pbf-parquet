#!/usr/bin/env bash
# Checks whether a crate's current Cargo.toml version is already published on
# crates.io, and sets the `already-published` step output accordingly.
#
# This is the whole publish gate for the merge-to-main workflow: a crate's
# Cargo.toml version only differs from what's already on crates.io when a PR
# bumped it, so "already published" and "needs publishing" are the same
# check. No release tag or other coordination is needed.
#
# Usage: check-crate-published.sh <crate-name> <manifest-path>
set -euo pipefail

crate_name="$1"
manifest_path="$2"

version=$(cargo metadata --no-deps --format-version=1 --manifest-path "$manifest_path" \
  | jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version')

if [ -z "$version" ]; then
  echo "::error::Could not read a version for crate '$crate_name' from $manifest_path" >&2
  exit 1
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
