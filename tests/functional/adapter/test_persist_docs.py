import json
import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsTest,
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass


class TestPersistDocsLateBinding(BasePersistDocsTest):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                    'view_model': {
                        'bind': False,
                    }
                }
            }
        }

    def test_comment_on_late_binding_view(self, project):
        run_dbt()
        run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 4
        table_node = catalog_data['nodes']['model.test.table_model']
        view_node = self._assert_has_table_comments(table_node)

        view_node = catalog_data['nodes']['model.test.view_model']
        self._assert_has_view_comments(view_node, False, False)

        no_docs_node = catalog_data['nodes']['model.test.no_docs_model']
        self._assert_has_view_comments(no_docs_node, False, False)
