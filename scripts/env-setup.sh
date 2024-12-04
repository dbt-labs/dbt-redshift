#!/bin/bash
# Set TOXENV environment variable for subsequent steps
echo "TOXENV=integration-redshift" >> $GITHUB_ENV
# Set INTEGRATION_TESTS_SECRETS_PREFIX environment variable for subsequent steps
# All GH secrets that have this prefix will be set as environment variables
echo "INTEGRATION_TESTS_SECRETS_PREFIX=REDSHIFT_TEST" >> $GITHUB_ENV
# Set environment variables required for integration tests
echo "DBT_TEST_USER_1=dbt_test_user_1" >> $GITHUB_ENV
echo "DBT_TEST_USER_2=dbt_test_user_2" >> $GITHUB_ENV
echo "DBT_TEST_USER_3=dbt_test_user_3" >> $GITHUB_ENV
echo "DBT_TEST_GROUP_1=dbt_test_group_1" >> $GITHUB_ENV
echo "DBT_TEST_GROUP_2=dbt_test_group_2" >> $GITHUB_ENV
echo "DBT_TEST_GROUP_3=dbt_test_group_3" >> $GITHUB_ENV
echo "DBT_TEST_ROLE_1=dbt_test_role_1" >> $GITHUB_ENV
echo "DBT_TEST_ROLE_2=dbt_test_role_2" >> $GITHUB_ENV
echo "DBT_TEST_ROLE_3=dbt_test_role_3" >> $GITHUB_ENV
