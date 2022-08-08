## dbt-redshift 1.3.0b2 (Release TBD)

### Fixes
- Avoid truncating Redshift model names with more than 63 characters([#147](https://github.com/dbt-labs/dbt-redshift/pull/147))

## dbt-redshift 1.3.0b1 (July 29, 2022)

### Under the hood
- Rename compiled_sql to compiled_code ([#145](https://github.com/dbt-labs/dbt-redshift/pull/145))

## dbt-redshift 1.2.0b1 (July 12, 2022)

### Features
- Add grants to materializations ([#128](https://github.com/dbt-labs/dbt-redshift/issues/128), [#131](https://github.com/dbt-labs/dbt-redshift/pull/131))

## dbt-redshift 1.2.0b1 (June 24, 2022)

### Under the hood
- Lift + shift for cross-db macros ([#120](https://github.com/dbt-labs/dbt-redshift/pull/120))
- Remove duplicated parts of unit test ([#100](https://github.com/dbt-labs/dbt-redshift/pull/100))

### Contributors
- [@dbeatty10](https://github.com/dbeatty10) ([#120](https://github.com/dbt-labs/dbt-redshift/pull/120))

## dbt-redshift 1.1.0 (April 28, 2022)

### Under the hood
- Add precommits for this repo ([#72](https://github.com/dbt-labs/dbt-redshift/issues/72), [#106](https://github.com/dbt-labs/dbt-redshift/pull/106))

## dbt-redshift 1.0.1 (April 19, 2022)

### Fixes
- Fix table creation statement ordering when including both the BACKUP parameter as well as the dist/sort keys ([#60](https://github.com/dbt-labs/dbt-redshift/issues/60), [#63](https://github.com/dbt-labs/dbt-redshift/pull/63))

### Under the hood
- Install compatible branch of dbt-core in unit/integration tests based on merge target ([#80](https://github.com/dbt-labs/dbt-redshift/pull/80))
- Fix test related to preventing coercion of boolean values (True,False) to numeric values (0,1) in query results ([#58](https://github.com/dbt-labs/dbt-redshift/pull/58))

### Contributors
- [@SMeltser](https://github.com/SMeltser) ([#63](https://github.com/dbt-labs/dbt-redshift/pull/63))

## dbt-redshift 1.1.0rc1 (April 13, 2022)

### Under the hood
- Use dbt.tests.adapter.basic in test suite ([#78](https://github.com/dbt-labs/dbt-redshift/issues/78), [#81](https://github.com/dbt-labs/dbt-redshift/pull/81))

## dbt-redshift 1.0.1rc1 (April 6, 2022)

### Fixes
- Fix table creation statement ordering when including both the BACKUP parameter as well as the dist/sort keys ([#60](https://github.com/dbt-labs/dbt-redshift/issues/60), [#63](https://github.com/dbt-labs/dbt-redshift/pull/63))

### Under the hood
- Install compatible branch of dbt-core in unit/integration tests based on merge target ([#80](https://github.com/dbt-labs/dbt-redshift/pull/80))
- Fix test related to preventing coercion of boolean values (True,False) to numeric values (0,1) in query results ([#58](https://github.com/dbt-labs/dbt-redshift/pull/58))

### Contributors
- [@SMeltser](https://github.com/SMeltser) ([#63](https://github.com/dbt-labs/dbt-redshift/pull/63))

## dbt-redshift 1.1.0b1 (March 23, 2022)

### Fixes
- Fix test related to preventing coercion of boolean values (True,False) to numeric values (0,1) in query results ([#58](https://github.com/dbt-labs/dbt-redshift/pull/58))
- Fix table creation statement ordering when including both the BACKUP parameter as well as the dist/sort keys ([#23](https://github.com/dbt-labs/dbt-redshift/issues/60)),([#63](https://github.com/dbt-labs/dbt-redshift/pull/63))
- Add unique\_id field to docs generation test catalogs; a follow-on PR to core PR ([#4168](https://github.com/dbt-labs/dbt-core/pull/4618)) and core PR ([#4701](https://github.com/dbt-labs/dbt-core/pull/4701))

### Under the hood
- Removes unused installs of dbt-core outside of tox env as it clutters up gha and can lead to misunderstanding of which version of dbt-core is being installed.([#90](https://github.com/dbt-labs/dbt-redshift/pull/90))
- Add stale pr/issue github action ([#65](https://github.com/dbt-labs/dbt-redshift/pull/65))
- Add env example file ([#69](https://github.com/dbt-labs/dbt-redshift/pull/69))

### Contributors
- [@SMeltser](https://github.com/SMeltser)([#63](https://github.com/dbt-labs/dbt-redshift/pull/63))

## dbt-redshift 1.0.0 (December 3, 2021)

## dbt-redshift 1.0.0rc2 (November 24, 2021)

### Under the hood
- Add optional Redshift parameter to create tables with BACKUP NO set, to exclude them from snapshots. ([#18](https://github.com/dbt-labs/dbt-redshift/issues/18), [#42](https://github.com/dbt-labs/dbt-redshift/pull/42))

### Contributors
- [@dlb8685](https://github.com/dlb8685) ([#42](https://github.com/dbt-labs/dbt-redshift/pull/42))

## dbt-redshift 1.0.0rc1 (November 10, 2021)

### Under the hood
- Remove official support for python 3.6, which is reaching end of life on December 23, 2021 ([dbt-core#4134](https://github.com/dbt-labs/dbt-core/issues/4134), [#38](https://github.com/dbt-labs/dbt-redshift/pull/38))
- Add support for structured logging [#34](https://github.com/dbt-labs/dbt-redshift/pull/34)

## dbt-redshift v1.0.0b2 (October 25, 2021)

### Under the hood
- Replace `sample_profiles.yml` with `profile_template.yml`, for use with new `dbt init` ([#29](https://github.com/dbt-labs/dbt-redshift/pull/29))

### Contributors
- [@NiallRees](https://github.com/NiallRees) ([#29](https://github.com/dbt-labs/dbt-redshift/pull/29))

## dbt-redshift v1.0.0b1 (October 11, 2021)

### Under the hood

- Initial adapter split out
