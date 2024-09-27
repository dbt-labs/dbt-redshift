from dbt.tests.util import get_model_file, relation_from_name, set_model_file


def update_model(project, name: str, model: str) -> str:
    relation = relation_from_name(project.adapter, name)
    original_model = get_model_file(project, relation)
    set_model_file(project, relation, model)
    return original_model
