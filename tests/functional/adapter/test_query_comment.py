import pytest
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class TestQueryCommentsRedshift(BaseQueryComments):
    pass

class TestMacroQueryCommentsRedshift(BaseMacroQueryComments):
    pass

class TestMacroArgsQueryCommentsRedshift(BaseMacroArgsQueryComments):
    pass

class TestMacroInvalidQueryCommentsRedshift(BaseMacroInvalidQueryComments):
    pass

class TestNullQueryCommentsRedshift(BaseNullQueryComments):
    pass

class TestEmptyQueryCommentsRedshift(BaseEmptyQueryComments):
    pass