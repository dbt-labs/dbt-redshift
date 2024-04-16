MY_SEED = """
id,name
1,apple
2,banana
3,cherry
""".strip()

MY_VIEW = """
select * from {{ ref("my_seed") }}
"""
