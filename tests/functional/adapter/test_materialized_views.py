from dbt.tests.adapter.materialized_view.base import Base, run_model


class TestBasic(Base):
    def test_relation_is_materialized_view_on_initial_creation(self, project):
        self.assert_relation_is_materialized_view(project)

    def test_relation_is_materialized_view_when_rerun(self, project):
        run_model(self.base_materialized_view.name)
        self.assert_relation_is_materialized_view(project)

    def test_relation_is_materialized_view_on_full_refresh(self, project):
        run_model(self.base_materialized_view.name, full_refresh=True)
        self.assert_relation_is_materialized_view(project)

    def test_relation_is_materialized_view_on_update(self, project):
        run_model(
            self.base_materialized_view.name, run_args=["--vars", "quoting: {identifier: True}"]
        )
        self.assert_relation_is_materialized_view(project)

    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        self.insert_records(project, self.inserted_records)
        assert self.get_records(project) == self.starting_records

        run_model(self.base_materialized_view.name)
        assert self.get_records(project) == self.starting_records + self.inserted_records
