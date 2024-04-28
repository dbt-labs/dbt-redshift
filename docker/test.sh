# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite
set -e

echo "\n\n"
echo "#######################################\n"
echo "##### Testing dbt-redshift latest #####\n"
echo "#######################################\n"

docker build --tag dbt-redshift --target dbt-redshift docker
docker run dbt-redshift --version

echo "\n\n"
echo "########################################\n"
echo "##### Testing dbt-redshift-1.0.0b1 #####\n"
echo "########################################\n"

docker build --tag dbt-redshift-1.0.0b1 --target dbt-redshift --build-arg commit_ref=v1.0.0b1 docker
docker run dbt-redshift-1.0.0b1 --version
