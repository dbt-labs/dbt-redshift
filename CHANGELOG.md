## dbt-redshift 1.0.1 (TBD)

### Fixes
- Fix test related to preventing coercion of boolean values (True,False) to numeric values (0,1) in query results ([#58](https://github.com/dbt-labs/dbt-redshift/blob/1.0.latest/CHANGELOG.md))
- Fix table creation statement ordering when including both the BACKUP parameter as well as the dist/sort keys ([#23](https://github.com/dbt-labs/dbt-redshift/issues/60))
- Add unique\_id field to docs generation test catalogs; a follow-on PR to core PR ([#4168](https://github.com/dbt-labs/dbt-core/pull/4618)) and core PR ([#4701](https://github.com/dbt-labs/dbt-core/pull/4701))

### Under the hood
- install compatiable branch of dbt-core in unit/integration tests based on merge target ([#80](https://github.com/dbt-labs/dbt-redshift/pull/80))

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
