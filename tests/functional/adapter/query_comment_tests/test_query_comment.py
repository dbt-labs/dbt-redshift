import pytest
from dbt.tests.adapter.query_comment.test_query_comment.py import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class BaseQueryCommentsRedshift(BaseQueryComments):
    pass

class BaseMacroQueryCommentsRedshift(BaseMacroQueryComments):
    pass

class BaseMacroArgsQueryCommentsRedshift(BaseMacroArgsQueryComments):
    pass

class BaseMacroInvalidQueryCommentsRedshift(BaseMacroInvalidQueryComments):
    pass

class BaseNullQueryCommentsRedshift(BaseNullQueryComments):
    pass

class BaseEmptyQueryCommentsRedshift(BaseEmptyQueryComments):
    pass