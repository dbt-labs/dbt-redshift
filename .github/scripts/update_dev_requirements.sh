#!/bin/bash -e
set -e

git_branch=$1
target_req_file="dev-requirements.txt"
req_sed_pattern="s/#egg=dbt-core/@${git_branch}#egg=dbt-core/g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$req_sed_pattern" $target_req_file
else
 sed -i "$req_sed_pattern" $target_req_file
fi
core_version=$(curl "https://raw.githubusercontent.com/dbt-labs/dbt-core/${git_branch}/core/dbt/version.py" | grep "__version__ = *"|cut -d'=' -f2)
bumpversion --allow-dirty --new-version "$core_version" major
