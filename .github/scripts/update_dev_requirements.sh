#!/bin/bash -e
set -e

git_branch=$1
target_file="dev-requirements.txt"
sed_pattern="s/#egg=dbt-core/@${git_branch}#egg=dbt-core/g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$sed_pattern" $target_file
else
 sed -i "$sed_pattern" $target_file
fi
