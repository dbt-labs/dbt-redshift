.DEFAULT_GOAL:=help

CI_FLAGS =\
	DBT_TEST_USER_1=$(if $(DBT_TEST_USER_1),$(DBT_TEST_USER_1),dbt_test_user_1)\
	DBT_TEST_USER_2=$(if $(DBT_TEST_USER_2),$(DBT_TEST_USER_2),dbt_test_user_2)\
	DBT_TEST_USER_3=$(if $(DBT_TEST_USER_3),$(DBT_TEST_USER_3),dbt_test_user_3)\
	DBT_TEST_GROUP_1=$(if $(DBT_TEST_GROUP_1),$(DBT_TEST_GROUP_1),dbt_test_group_1)\
	DBT_TEST_GROUP_2=$(if $(DBT_TEST_GROUP_2),$(DBT_TEST_GROUP_2),dbt_test_group_2)\
	DBT_TEST_GROUP_3=$(if $(DBT_TEST_GROUP_3),$(DBT_TEST_GROUP_3),dbt_test_group_3)\
	DBT_TEST_ROLE_1=$(if $(DBT_TEST_ROLE_1),$(DBT_TEST_ROLE_1),dbt_test_role_1)\
	DBT_TEST_ROLE_2=$(if $(DBT_TEST_ROLE_2),$(DBT_TEST_ROLE_2),dbt_test_role_2)\
	DBT_TEST_ROLE_3=$(if $(DBT_TEST_ROLE_3),$(DBT_TEST_ROLE_3),dbt_test_role_3)\
	RUSTFLAGS=$(if $(RUSTFLAGS),$(RUSTFLAGS),"-D warnings")\
	LOG_DIR=$(if $(LOG_DIR),$(LOG_DIR),./logs)\
	DBT_LOG_FORMAT=$(if $(DBT_LOG_FORMAT),$(DBT_LOG_FORMAT),json)

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	pip install -e . -r dev-requirements.txt && pre-commit install

.PHONY: dev-uninstall
dev-uninstall: ## Uninstalls all packages while maintaining the virtual environment
               ## Useful when updating versions, or if you accidentally installed into the system interpreter
	pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y
	pip uninstall -y dbt-redshift

.PHONY: mypy
mypy: ## Runs mypy against staged changes for static type checking.
	@\
	pre-commit run --hook-stage manual mypy-check | grep -v "INFO"

.PHONY: flake8
flake8: ## Runs flake8 against staged changes to enforce style guide.
	@\
	pre-commit run --hook-stage manual flake8-check | grep -v "INFO"

.PHONY: black
black: ## Runs black  against staged changes to enforce style guide.
	@\
	pre-commit run --hook-stage manual black-check -v | grep -v "INFO"

.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes.
	@\
	pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: linecheck
linecheck: ## Checks for all Python lines 100 characters or more
	@\
	find dbt -type f -name "*.py" -exec grep -I -r -n '.\{100\}' {} \;

.PHONY: unit
unit: ## Runs unit tests with py38.
	@\
	tox -e py38

.PHONY: test
test: ## Runs unit tests with py38 and code checks against staged changes.
	@\
	tox -p -e py38; \
	pre-commit run black-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: integration
integration: ## Runs redshift integration tests with py38.
	@\
	tox -e py38-redshift --

.PHONY: clean
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


.PHONY: docker-dev
docker-dev:
	docker build -f docker/dev.Dockerfile -t dbt-redshift-dev .
	docker run --rm -it --name dbt-redshift-dev -v $(shell pwd):/opt/code dbt-redshift-dev

.PHONY: docker-prod
docker-prod:
	docker build -f docker/Dockerfile -t dbt-redshift .
