import pytest
import sys
import importlib


@pytest.fixture
def remove_agate_from_path():
    """conftest and other envs load agate modules upon initilization so we need
    to remove their presence from module tracking to assess correctness of direct imports"""

    original_sys_modules = sys.modules.copy()

    # import ahead of time to avoid reimporting agate upon package initialization
    import dbt.adapters.redshift.__init__

    modules_to_remove = [m for m in sys.modules if "agate" in m]
    for m in modules_to_remove:
        del sys.modules[m]

    yield
    sys.modules = original_sys_modules


def test_lazy_loading_agate(remove_agate_from_path):
    """If agate is imported directly here or in any of the subsequent files, this test will fail. Also test that our assumptions about imports affecting sys modules hold."""
    import dbt

    importlib.reload(dbt.adapters.redshift.connections)
    importlib.reload(dbt.adapters.redshift.impl)
    importlib.reload(dbt.adapters.redshift.relation_configs.base)
    importlib.reload(dbt.adapters.redshift.relation_configs.dist)
    importlib.reload(dbt.adapters.redshift.relation_configs.materialized_view)
    importlib.reload(dbt.adapters.redshift.relation_configs.sort)

    assert not any([module_name for module_name in sys.modules if "agate" in module_name])

    import agate

    assert any([module_name for module_name in sys.modules if "agate" in module_name])
