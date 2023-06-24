import pytest

from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.contracts.relation import RelationType
from dbt.contracts.results import RunStatus
from dbt.tests.adapter.materialized_view.base import (
    run_model,
    assert_model_exists_and_is_correct_type,
    insert_record,
    get_row_count,
)
from dbt.tests.adapter.materialized_view.on_configuration_change import (
    assert_proper_scenario,
)

from tests.functional.adapter.materialized_view_tests.fixtures import (
    RedshiftBasicBase,
    RedshiftOnConfigurationChangeBase,
    refresh_materialized_view,
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

    def test_updated_base_table_data_only_shows_in_materialized_view_after_refresh(self, project):
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
        refresh_materialized_view(project, "base_materialized_view")

        # poll database
        table_end = get_row_count(project, "base_table")
        view_end = get_row_count(project, "base_materialized_view")
        assert view_end == table_end

        # new records were inserted in the table but didn't show up in the view until it was refreshed
        assert table_start < table_mid == table_end
        assert view_start == view_mid < view_end


class TestOnConfigurationChangeApply(RedshiftOnConfigurationChangeBase):
    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self,
        configuration_changes_apply,
        configuration_changes_refresh,
        replace_message,
        configuration_change_message,
    ):
        results, logs = run_model("base_materialized_view", full_refresh=True)
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )

    def test_model_applies_changes_with_small_configuration_changes(
        self, configuration_changes_apply, alter_message, update_auto_refresh_message
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, update_auto_refresh_message],
        )

    def test_model_rebuilds_with_large_configuration_changes(
        self, configuration_changes_refresh, alter_message, replace_message
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, replace_message],
        )

    def test_model_only_rebuilds_with_large_configuration_changes(
        self,
        configuration_changes_apply,
        configuration_changes_refresh,
        alter_message,
        replace_message,
        update_auto_refresh_message,
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, replace_message],
            messages_not_in_logs=[update_auto_refresh_message],
        )


class TestOnConfigurationChangeContinue(RedshiftOnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Continue.value}}

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self,
        configuration_changes_apply,
        configuration_changes_refresh,
        replace_message,
        configuration_change_message,
    ):
        results, logs = run_model("base_materialized_view", full_refresh=True)
        assert_proper_scenario(
            OnConfigurationChangeOption.Continue,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Continue,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )

    def test_model_is_skipped_with_configuration_changes(
        self, configuration_changes_apply, configuration_change_continue_message
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Continue,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[configuration_change_continue_message],
        )


class TestOnConfigurationChangeFail(RedshiftOnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Fail.value}}

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self,
        configuration_changes_apply,
        configuration_changes_refresh,
        replace_message,
        configuration_change_message,
    ):
        results, logs = run_model("base_materialized_view", full_refresh=True)
        assert_proper_scenario(
            OnConfigurationChangeOption.Fail,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message
    ):
        results, logs = run_model("base_materialized_view")
        assert_proper_scenario(
            OnConfigurationChangeOption.Fail,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )

    def test_run_fails_with_configuration_changes(
        self, configuration_changes_apply, configuration_change_fail_message
    ):
        results, logs = run_model("base_materialized_view", expect_pass=False)
        assert_proper_scenario(
            OnConfigurationChangeOption.Fail,
            results,
            logs,
            RunStatus.Error,
            messages_in_logs=[configuration_change_fail_message],
        )
