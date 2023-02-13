from typing import Dict, List, Iterable

import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter import common
from tests.functional.adapter.snapshot_tests import files


class SnapshotBase:

    @pytest.fixture(scope="class")
    def seeds(self):
        """
        This seed file contains all records needed for tests, including records which will be inserted after the
        initial snapshot. This makes it so that Redshift creates the correct size varchar columns. This table
        will only need to be loaded once at the class level. It will never be altered, hence requires no further
        setup or teardown.
        """
        return {"seed.csv": files.SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        """
        This will be the working base table. It will be altered by each test, hence will require setup and
        teardown at the test case level.
        """
        return {"fact.sql": files.MODEL_FACT_SQL}

    @pytest.fixture(scope="class", autouse=True)
    def _setup_class(self, project):
        """
        Load `seed` once for the whole class
        """
        run_dbt(["seed"])

    @pytest.fixture(scope="function", autouse=True)
    def _setup_method(self, project):
        """
        Initialize `fact` and `snapshot` for every test case.
        Only load the first 20 `seed` records into `fact`; withhold 10 records as "new" (e.g. to test inserts).

        Make the project a class variable to simplify function calls and make the code more readable.
        For some reason this doesn't work in the class-scoped fixture, but does in the function-scoped fixture.
        """
        self.project = project
        self.create_fact_from_seed("id between 1 and 20")
        run_dbt(["snapshot"])
        yield
        self.delete_snapshot_records()
        self.delete_fact_records()

    def update_fact_records(self, updates: Dict[str, str], where: str = None):
        common.update_records(self.project, "fact", updates, where)

    def insert_fact_records(self, where: str = None):
        common.insert_records(self.project, "fact", "seed", "*", where)

    def delete_fact_records(self, where: str = None):
        common.delete_records(self.project, "fact", where)

    def add_fact_column(self, column: str = None, definition: str = None):
        common.add_column(self.project, "fact", column, definition)

    def create_fact_from_seed(self, where: str = None):
        common.clone_table(self.project, "fact", "seed", "*", where)

    def get_snapshot_records(self, select: str = None, where: str = None) -> List[tuple]:
        return common.get_records(self.project, "snapshot", select, where)

    def delete_snapshot_records(self):
        common.delete_records(self.project, "snapshot")

    def _assert_results(
            self,
            ids_with_current_snapshot_records: Iterable,
            ids_with_closed_out_snapshot_records: Iterable
    ):
        """
        All test cases are checked by considering whether a source record's id is end dated in `snapshot`. Each id
        can fall into one of the following cases:

        - The id is end-dated
            - the record was hard deleted
        - The id is not end-dated
            - attribution is currently applicable
        - The id is end-dated on one record, but not on the other record
            - there is out-dated attribution which was updated with current attribution
            - the record was hard deleted and revived

        Note: Because of the third scenario, ids may show up in both arguments of this method.

        Args:
            ids_with_current_snapshot_records: a list/set/etc. of ids which are not end-dated
            ids_with_closed_out_snapshot_records: a list/set/etc. of ids which are end-dated
        """
        records = set(self.get_snapshot_records("id, dbt_valid_to is null as is_current"))
        expected_records = set().union(
            {(i, True) for i in ids_with_current_snapshot_records},
            {(i, False) for i in ids_with_closed_out_snapshot_records}
        )
        assert records == expected_records


class TestSnapshot(SnapshotBase):

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": files.SNAPSHOT_TIMESTAMP_SQL}

    def test_updates_are_captured_by_snapshot(self, project):
        """
        Update the last 5 records. Show that all ids are current, but the last 5 reflect updates.
        """
        self.update_fact_records({"updated_at": "updated_at + interval '1 day'"}, "id between 16 and 20")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(16, 21)
        )

    def test_inserts_are_captured_by_snapshot(self, project):
        """
        Insert 10 records. Show that there are 30 records in `snapshot`, all of which are current.
        """
        self.insert_fact_records("id between 21 and 30")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 31),
            ids_with_closed_out_snapshot_records=[]
        )

    def test_deletes_are_captured_by_snapshot(self, project):
        """
        Hard delete the last five records. Show that there are now only 15 current records and 5 end-dated records.
        """
        self.delete_fact_records("id between 16 and 20")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 16),
            ids_with_closed_out_snapshot_records=range(16, 21)
        )

    def test_revives_are_captured_by_snapshot(self, project):
        """
        Delete the last five records and run snapshot to collect that information, then revive 3 of those records.
        Show that there are now 18 current records and 5 end-dated records.
        """
        self.delete_fact_records("id between 16 and 20")
        run_dbt(["snapshot"])
        self.insert_fact_records("id between 16 and 18")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 19),
            ids_with_closed_out_snapshot_records=range(16, 21)
        )

    def test_new_column_captured_by_snapshot(self, project):
        """
        Add a column to `fact` and populate the last 10 records with a non-null value.
        Show that all ids are current, but the last 10 reflect updates and the first 10 do not.
            i.e. if the column is added, but not updated, the record does not reflect that it's updated
        """
        self.add_fact_column("full_name", "varchar(200) default null")
        self.update_fact_records(
            {
                "full_name": "first_name || ' ' || last_name",
                "updated_at": "updated_at + interval '1 day'",
            },
            "id between 11 and 20"
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(11, 21)
        )


class TestSnapshotCheck(SnapshotBase):

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": files.SNAPSHOT_CHECK_SQL}

    def test_column_selection_is_reflected_in_snapshot(self, project):
        """
        Update the first 10 records on a non-tracked column.
        Update the middle 10 records on a tracked column. (hence records 6-10 are updated on both)
        Show that all ids are current, and only the tracked column updates are reflected in `snapshot`.
        """
        self.update_fact_records({"last_name": "left(last_name, 3)"}, "id between 1 and 10")  # not tracked
        self.update_fact_records({"email": "left(email, 3)"}, "id between 6 and 15")          # tracked
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(6, 16)
        )
