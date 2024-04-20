#!/bin/bash -e
set -e

package=$1
branch=$2

file="pyproject.toml"
sed_pattern="s|${package}.git@main*|${package}.git@${branch}|g"

# mac ships with a different version of sed that requires a delimiter arg
if [[ "$OSTYPE" == darwin* ]]; then
    sed -i "" "$sed_pattern" $file
else
    sed -i "$sed_pattern" $file
fi
