import os
import pytest
import unittest

from unittest import mock

from .utils import config_from_parts_or_dicts, inject_adapter, clear_plugin
from .mock_adapter import adapter_factory
import dbt.exceptions

from dbt.adapters import (
    redshift,
    factory,
)
from dbt.contracts.graph.model_config import (
    NodeConfig,
)
from dbt.contracts.graph.nodes import ModelNode, DependsOn, Macro
from dbt.context import providers
from dbt.node_types import NodeType


class TestRuntimeWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_config = mock.MagicMock()
        self.mock_config.quoting = {"database": True, "schema": True, "identifier": True}
        adapter_class = adapter_factory()
        self.mock_adapter = adapter_class(self.mock_config)
        self.namespace = mock.MagicMock()
        self.wrapper = providers.RuntimeDatabaseWrapper(self.mock_adapter, self.namespace)
        self.responder = self.mock_adapter.responder


PROFILE_DATA = {
    "target": "test",
    "quoting": {},
    "outputs": {
        "test": {
            "type": "redshift",
            "host": "localhost",
            "schema": "analytics",
            "user": "test",
            "pass": "test",
            "dbname": "test",
            "port": 1,
        }
    },
}


PROJECT_DATA = {
    "name": "root",
    "version": "0.1",
    "profile": "test",
    "project-root": os.getcwd(),
    "config-version": 2,
}


def model():
    return ModelNode(
        alias="model_one",
        name="model_one",
        database="dbt",
        schema="analytics",
        resource_type=NodeType.Model,
        unique_id="model.root.model_one",
        fqn=["root", "model_one"],
        package_name="root",
        original_file_path="model_one.sql",
        root_path="/usr/src/app",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "view",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
            }
        ),
        tags=[],
        path="model_one.sql",
        raw_sql="",
        description="",
        columns={},
    )


def mock_macro(name, package_name):
    macro = mock.MagicMock(
        __class__=Macro,
        package_name=package_name,
        resource_type="macro",
        unique_id=f"macro.{package_name}.{name}",
    )
    # Mock(name=...) does not set the `name` attribute, this does.
    macro.name = name
    return macro


def mock_manifest(config):
    manifest_macros = {}
    for name in ["macro_a", "macro_b"]:
        macro = mock_macro(name, config.project_name)
        manifest_macros[macro.unique_id] = macro
    return mock.MagicMock(macros=manifest_macros)


def mock_model():
    return mock.MagicMock(
        __class__=ModelNode,
        alias="model_one",
        name="model_one",
        database="dbt",
        schema="analytics",
        resource_type=NodeType.Model,
        unique_id="model.root.model_one",
        fqn=["root", "model_one"],
        package_name="root",
        original_file_path="model_one.sql",
        root_path="/usr/src/app",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "view",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
            }
        ),
        tags=[],
        path="model_one.sql",
        raw_sql="",
        description="",
        columns={},
    )


@pytest.fixture
def get_adapter():
    with mock.patch.object(providers, "get_adapter") as patch:
        yield patch


@pytest.fixture
def get_include_paths():
    with mock.patch.object(factory, "get_include_paths") as patch:
        patch.return_value = []
        yield patch


@pytest.fixture
def config():
    return config_from_parts_or_dicts(PROJECT_DATA, PROFILE_DATA)


@pytest.fixture
def manifest_fx(config):
    return mock_manifest(config)


@pytest.fixture
def manifest_extended(manifest_fx):
    dbt_macro = mock_macro("default__some_macro", "dbt")
    # same namespace, same name, different pkg!
    rs_macro = mock_macro("redshift__some_macro", "dbt_redshift")
    # same name, different package
    package_default_macro = mock_macro("default__some_macro", "root")
    package_rs_macro = mock_macro("redshift__some_macro", "root")
    manifest_fx.macros[dbt_macro.unique_id] = dbt_macro
    manifest_fx.macros[rs_macro.unique_id] = rs_macro
    manifest_fx.macros[package_default_macro.unique_id] = package_default_macro
    manifest_fx.macros[package_rs_macro.unique_id] = package_rs_macro
    return manifest_fx


@pytest.fixture
def redshift_adapter(config, get_adapter):
    adapter = redshift.RedshiftAdapter(config)
    inject_adapter(adapter, redshift.Plugin)
    get_adapter.return_value = adapter
    yield adapter
    clear_plugin(redshift.Plugin)


def test_resolve_specific(config, manifest_extended, redshift_adapter, get_include_paths):
    rs_macro = manifest_extended.macros["macro.dbt_redshift.redshift__some_macro"]
    package_rs_macro = manifest_extended.macros["macro.root.redshift__some_macro"]

    ctx = providers.generate_runtime_model_context(
        model=mock_model(),
        config=config,
        manifest=manifest_extended,
    )

    ctx["adapter"].config.dispatch

    # macro_a exists, but default__macro_a and redshift__macro_a do not
    with pytest.raises(dbt.exceptions.CompilationError):
        ctx["adapter"].dispatch("macro_a").macro

    # root namespace is always preferred, unless search order is explicitly defined in 'dispatch' config
    assert ctx["adapter"].dispatch("some_macro").macro is package_rs_macro
    assert ctx["adapter"].dispatch("some_macro", "dbt").macro is package_rs_macro
    assert ctx["adapter"].dispatch("some_macro", "root").macro is package_rs_macro

    # override 'dbt' namespace search order, dispatch to 'root' first
    ctx["adapter"].config.dispatch = [{"macro_namespace": "dbt", "search_order": ["root", "dbt"]}]
    assert ctx["adapter"].dispatch("some_macro", macro_namespace="dbt").macro is package_rs_macro

    # override 'dbt' namespace search order, dispatch to 'dbt' only
    ctx["adapter"].config.dispatch = [{"macro_namespace": "dbt", "search_order": ["dbt"]}]
    assert ctx["adapter"].dispatch("some_macro", macro_namespace="dbt").macro is rs_macro

    # override 'root' namespace search order, dispatch to 'dbt' first
    ctx["adapter"].config.dispatch = [{"macro_namespace": "root", "search_order": ["dbt", "root"]}]
