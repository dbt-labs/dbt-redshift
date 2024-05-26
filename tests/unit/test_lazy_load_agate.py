import pytest
import sys


@pytest.fixture
def remove_agate_from_path():
    original_sys_path = sys.modules.copy()

    # conftest loads agate modules so we need to remove them
    # and this file

    # import ahead of time to avoid reimporting agate upon package initialization
    import dbt.adapters.redshift.__init__

    modules_to_remove = [m for m in sys.modules if "agate" in m]
    for m in modules_to_remove:
        del sys.modules[m]

    yield
    sys.path = original_sys_path


def test_lazy_loading_agate(remove_agate_from_path):
    """If agate is imported directly here or in any of the subsequent files, this test will fail. Also test that our assumptions about imports affecting sys modules hold."""
    import dbt.adapters.redshift.connections
    import dbt.adapters.redshift.impl
    import dbt.adapters.redshift.relation_configs.base
    import dbt.adapters.redshift.relation_configs.dist
    import dbt.adapters.redshift.relation_configs.materialized_view
    import dbt.adapters.redshift.relation_configs.sort

    assert not any([module_name for module_name in sys.modules if "agate" in module_name])

    import agate

    assert any([module_name for module_name in sys.modules if "agate" in module_name])
