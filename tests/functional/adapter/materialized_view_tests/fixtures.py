import pytest

from dbt.tests.util import relation_from_name
from dbt.tests.adapter.materialized_view.base import Base
from dbt.tests.adapter.materialized_view.on_configuration_change import (
    OnConfigurationChangeBase,
    get_model_file,
    set_model_file,
)


class RedshiftBasicBase(Base):
    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(materialized='table') }}
        select 1 as base_column
        """
        base_materialized_view = """
        {{ config(materialized='materialized_view') }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_materialized_view.sql": base_materialized_view}


class RedshiftOnConfigurationChangeBase(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(
            materialized='table',
        ) }}
        select
            1 as id,
            100 as value,
            42 as new_id,
            4242 as new_value
        """
        base_materialized_view = """
        {{ config(
            materialized='materialized_view',
        ) }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_materialized_view.sql": base_materialized_view}

    @pytest.fixture(scope="function")
    def configuration_changes_apply(self, project):
        initial_model = get_model_file(project, "base_materialized_view")

        # turn on auto_refresh
        new_model = initial_model.replace(
            "materialized='materialized_view',",
            "materialized='materialized_view', auto_refresh='yes',",
        )
        set_model_file(project, "base_materialized_view", new_model)

        yield

        # set this back for the next test
        set_model_file(project, "base_materialized_view", initial_model)

    @pytest.fixture(scope="function")
    def configuration_changes_refresh(self, project):
        initial_model = get_model_file(project, "base_materialized_view")

        # add a sort_key
        new_model = initial_model.replace(
            "materialized='materialized_view',",
            "materialized='materialized_view', sort='id',",
        )
        set_model_file(project, "base_materialized_view", new_model)

        yield

        # set this back for the next test
        set_model_file(project, "base_materialized_view", initial_model)

    @pytest.fixture(scope="function")
    def update_auto_refresh_message(self, project):
        return f"Applying UPDATE AUTO_REFRESH to: {relation_from_name(project.adapter, 'base_materialized_view')}"
