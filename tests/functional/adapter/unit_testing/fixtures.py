model_null_value_base = """
{{ config(materialized="table") }}

select 1 as id, 'a' as col1
"""

model_null_value_model = """
{{config(materialized="table")}}

select * from {{ ref('null_value_base') }}
"""


test_null_column_value_doesnt_throw_error_csv = """
unit_tests:
  - name: test_simple

    model: null_value_model
    given:
      - input: ref('null_value_base')
        format: csv
        rows: |
          id,col1
          ,d
          ,e
          6,f

    expect:
        format: csv
        rows: |
          id,col1
          ,d
          ,e
          6,f
"""

test_null_column_value_doesnt_throw_error_dct = """
unit_tests:
  - name: test_simple

    model: null_value_model
    given:
      - input: ref('null_value_base')
        rows:
          - { "id":  , "col1": "d"}
          - { "id":  , "col1": "e"}
          - { "id": 6, "col1": "f"}

    expect:
      rows:
        - {id:  , "col1": "d"}
        - {id:  , "col1": "e"}
        - {id: 6, "col1": "f"}
"""

test_null_column_value_will_throw_error = """
unit_tests:
  - name: test_simple

    model: null_value_model
    given:
      - input: ref('null_value_base')
        rows:
          - { "id":  , "col1": "d"}
          - { "id":  , "col1": "e"}
          - { "id": 6, "col1": }

    expect:
      rows:
        - {id:  , "col1": "d"}
        - {id:  , "col1": "e"}
        - {id: 6, "col1": }
"""
