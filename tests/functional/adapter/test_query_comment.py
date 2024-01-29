from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)
from dbt.adapters.redshift.__version__ import version
import json


class TestQueryCommentsRedshift(BaseQueryComments):
    pass


class TestMacroQueryCommentsRedshift(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsRedshift(BaseMacroArgsQueryComments):
    def test_matches_comment(self, project):
        logs = self.run_get_json()
        expected_dct = {
            "app": "dbt++",
            "dbt_version": version,
            "macro_version": "0.1.0",
            "message": f"blah: {project.adapter.config.target_name}",
        }
        expected = r"/* {} */\n".format(json.dumps(expected_dct, sort_keys=True)).replace(
            '"', r"\""
        )
        assert expected in logs


class TestMacroInvalidQueryCommentsRedshift(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsRedshift(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsRedshift(BaseEmptyQueryComments):
    pass
