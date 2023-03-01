#!/bin/bash -e
set -e

release_branch="1.5.latest"
target_req_file=".github/workflows/nightly-release.yml"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "s|(\d+\.)?(\d+\.).latest|${release_branch}" $target_req_file
else
 sed -i "s|(\d+\.)?(\d+\.).latest|${release_branch}" $target_req_file
fi
