# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite

clear \
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-redshift latest #####\n"\
"########################################\n"\
&& docker build --tag dbt-redshift \
  --target dbt-redshift \
  docker \
&& docker run dbt-redshift --version \
\
&& echo "\n\n"\
"#########################################\n"\
"##### Testing dbt-redshift-1.0.0b1 #####\n"\
"#########################################\n"\
&& docker build --tag dbt-redshift-1.0.0b1 \
  --target dbt-redshift \
  --build-arg dbt_redshift_ref=dbt-redshift@v1.0.0b1 \
  docker \
&& docker run dbt-redshift-1.0.0b1 --version
