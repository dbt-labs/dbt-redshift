from datetime import datetime
import time

import pytest

from dbt.tests.adapter.materialized_view.auto_refresh import (
    MaterializedViewAutoRefreshNoChanges,
)

from tests.functional.adapter.materialized_view_tests import files


class TestMaterializedViewAutoRefreshNoChanges(MaterializedViewAutoRefreshNoChanges):
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "auto_refresh_on.sql": files.MY_MATERIALIZED_VIEW_ON,
            "auto_refresh_off.sql": files.MY_MATERIALIZED_VIEW_OFF,
        }

    @pytest.fixture(scope="class", autouse=True)
    def macros(self):
        yield {"redshift__test__last_refresh.sql": files.MACRO__LAST_REFRESH}

    def last_refreshed(self, project, materialized_view: str) -> datetime:
        with project.adapter.connection_named("__test"):
            kwargs = {"schema": project.test_schema, "identifier": materialized_view}
            last_refresh_results = project.adapter.execute_macro(
                "redshift__test__last_refresh", kwargs=kwargs
            )
        if len(last_refresh_results) > 0:
            last_refresh = last_refresh_results[0].get("last_refresh")
        else:
            # redshift doesn't store the first refresh, so assume it's the beginning of time
            # this should be the created date of the materialized view, but redshift doesn't
            # make that available to the user for a materialized view
            last_refresh = datetime.fromtimestamp(time.mktime(time.gmtime(0)))
        return last_refresh
