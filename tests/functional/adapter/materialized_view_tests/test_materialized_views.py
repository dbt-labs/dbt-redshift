from dbt.contracts.relation import RelationType
from dbt.tests.adapter.materialized_view.base import (
    run_model,
    assert_model_exists_and_is_correct_type,
    insert_record,
    get_row_count,
)

from tests.functional.adapter.materialized_view_tests.fixtures import (
    RedshiftBasicBase,
)


class TestBasic(RedshiftBasicBase):
    def test_relation_is_materialized_view_on_initial_creation(self, project):
        assert_model_exists_and_is_correct_type(
            project, "base_materialized_view", RelationType.MaterializedView
        )
        assert_model_exists_and_is_correct_type(project, "base_table", RelationType.Table)

    def test_relation_is_materialized_view_when_rerun(self, project):
        run_model("base_materialized_view")
        assert_model_exists_and_is_correct_type(
            project, "base_materialized_view", RelationType.MaterializedView
        )

    def test_relation_is_materialized_view_on_full_refresh(self, project):
        run_model("base_materialized_view", full_refresh=True)
        assert_model_exists_and_is_correct_type(
            project, "base_materialized_view", RelationType.MaterializedView
        )

    def test_relation_is_materialized_view_on_update(self, project):
        run_model("base_materialized_view", run_args=["--vars", "quoting: {identifier: True}"])
        assert_model_exists_and_is_correct_type(
            project, "base_materialized_view", RelationType.MaterializedView
        )

    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        # poll database
        table_start = get_row_count(project, "base_table")
        view_start = get_row_count(project, "base_materialized_view")
        assert view_start == table_start

        # insert new record in table
        new_record = (2,)
        insert_record(project, new_record, "base_table", ["base_column"])

        # poll database
        table_mid = get_row_count(project, "base_table")
        view_mid = get_row_count(project, "base_materialized_view")

        # refresh the materialized view
        run_model("base_materialized_view")

        # poll database
        table_end = get_row_count(project, "base_table")
        view_end = get_row_count(project, "base_materialized_view")
        assert view_end == table_end

        # new records were inserted in the table but didn't show up in the view until it was refreshed
        assert table_start < table_mid == table_end
        assert view_start == view_mid < view_end
