from typing import Set, Tuple

import pytest

from dbt.tests.util import run_dbt, relation_from_name
from dbt.tests.fixtures.project import TestProjInfo


_SEED = """
id,first_name,last_name,email,gender,ip_address,updated_at
1,Judith,Kennedy,jkennedy0@phpbb.com,Female,54.60.24.128,2015-12-24
2,Arthur,Kelly,akelly1@eepurl.com,Male,62.56.24.215,2015-10-28
3,Rachel,Moreno,rmoreno2@msu.edu,Female,31.222.249.23,2016-04-05
4,Ralph,Turner,rturner3@hp.com,Male,157.83.76.114,2016-08-08
5,Laura,Gonzales,lgonzales4@howstuffworks.com,Female,30.54.105.168,2016-09-01
6,Katherine,Lopez,klopez5@yahoo.co.jp,Female,169.138.46.89,2016-08-30
7,Jeremy,Hamilton,jhamilton6@mozilla.org,Male,231.189.13.133,2016-07-17
8,Heather,Rose,hrose7@goodreads.com,Female,87.165.201.65,2015-12-29
9,Gregory,Kelly,gkelly8@trellian.com,Male,154.209.99.7,2016-03-24
10,Rachel,Lopez,rlopez9@themeforest.net,Female,237.165.82.71,2016-08-20
11,Donna,Welch,dwelcha@shutterfly.com,Female,103.33.110.138,2016-02-27
12,Russell,Lawrence,rlawrenceb@qq.com,Male,189.115.73.4,2016-06-11
13,Michelle,Montgomery,mmontgomeryc@scientificamerican.com,Female,243.220.95.82,2016-06-18
14,Walter,Castillo,wcastillod@pagesperso-orange.fr,Male,71.159.238.196,2016-10-06
15,Robin,Mills,rmillse@vkontakte.ru,Female,172.190.5.50,2016-10-31
16,Raymond,Holmes,rholmesf@usgs.gov,Male,148.153.166.95,2016-10-03
17,Gary,Bishop,gbishopg@plala.or.jp,Male,161.108.182.13,2016-08-29
18,Anna,Riley,arileyh@nasa.gov,Female,253.31.108.22,2015-12-11
19,Sarah,Knight,sknighti@foxnews.com,Female,222.220.123.177,2016-09-26
20,Phyllis,Fox,null,Female,163.191.232.95,2016-08-21
21,Judy,Robinson,jrobinsonk@blogs.com,Female,208.21.192.232,2016-09-18
22,Kevin,Alvarez,kalvarezl@buzzfeed.com,Male,228.106.146.9,2016-07-29
23,Barbara,Carr,bcarrm@pen.io,Female,106.165.140.17,2015-09-24
24,William,Watkins,wwatkinsn@guardian.co.uk,Male,78.155.84.6,2016-03-08
25,Judy,Cooper,jcoopero@google.com.au,Female,24.149.123.184,2016-10-05
26,Shirley,Castillo,scastillop@samsung.com,Female,129.252.181.12,2016-06-20
27,Justin,Harper,jharperq@opera.com,Male,131.172.103.218,2016-05-21
28,Marie,Medina,mmedinar@nhs.uk,Female,188.119.125.67,2015-10-08
29,Kelly,Edwards,kedwardss@phoca.cz,Female,47.121.157.66,2015-09-15
30,Carl,Coleman,ccolemant@wikipedia.org,Male,82.227.154.83,2016-05-26
""".strip()


_MODEL_FACT = """
{{ config(materialized="table") }}
select *
from {{ ref('seed') }}
where id between 1 and 20
"""


_MODEL_RESULTS = """
{{ config(materialized="view") }}
select id,
       dbt_valid_to is null as is_current
from {{ ref('snapshot') }}
"""


_SNAPSHOT_TIMESTAMP = """
{% snapshot snapshot %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""


_SNAPSHOT_CHECK = """
{% snapshot snapshot %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='check',
        check_cols=['email'],
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""


_MACROS = """
{% macro update_records(range) %}
    {% set sql %}
    
        update {{ ref('fact') }}
        set updated_at = updated_at + interval '1 day'
        where id between {{ range }}
        
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro insert_records(range) %}
    {% set sql %}

        insert into {{ ref('fact') }}
        select * from {{ ref('seed') }}
        where id between {{ range }}
        
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro delete_records(range) %}
    {% set sql %}

        delete from {{ ref('fact') }}
        where id between {{ range }}
        
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro add_a_column() %}
    {% set sql %}

        alter table {{ ref('fact') }}
        add column full_name varchar(200) default null
        ;

        update {{ ref('fact') }}
        set full_name = first_name || ' ' || last_name,
            updated_at = updated_at + interval '1 day'
        ;
        
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro update_tracked_and_untracked_columns(tracked_range, untracked_range) %}
    {% set sql %}
    
        update {{ ref('fact') }}
        set last_name = left(last_name, 3)
        where id between {{ untracked_range }}
        ;

        update {{ ref('fact') }}
        set email = left(email, 3)
        where id between {{ tracked_range }}
        ;
        
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
"""


class SnapshotBase:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact.sql": _MODEL_FACT,
            "results.sql": _MODEL_RESULTS,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": _MACROS}

    @pytest.fixture(scope="class", autouse=True)
    def _setup_class(self, project: TestProjInfo):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "fact"])
        run_dbt(["snapshot"])
        run_dbt(["run", "--select", "results"])
        project.run_sql(f"""delete from {relation_from_name(project.adapter, "snapshot")}""")

    @pytest.fixture(scope="function", autouse=True)
    def _setup_method(self, project: TestProjInfo):
        run_dbt(["run", "--select", "fact"])
        run_dbt(["snapshot"])
        yield
        project.run_sql(f"""delete from {relation_from_name(project.adapter, "snapshot")}""")

    @staticmethod
    def _results(project: TestProjInfo) -> Set[Tuple[str, bool]]:
        return set(project.run_sql(f"select * from {relation_from_name(project.adapter, 'results')}", fetch="all"))


class TestSnapshot(SnapshotBase):

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _SNAPSHOT_TIMESTAMP}

    @pytest.mark.parametrize("operation,run_args,current_records,closed_records", [
        ("update_records", "{range: '16 and 20'}", range(1, 21), range(16, 21)),
        ("insert_records", "{range: '21 and 30'}", range(1, 31), []),
        ("delete_records", "{range: '16 and 20'}", range(1, 16), range(16, 21)),
        ("add_a_column", "{}", range(1, 21), range(1, 21)),
    ])
    def test_crud_operations_are_captured_by_snapshot(
            self,
            project: TestProjInfo,
            operation,
            run_args,
            current_records,
            closed_records
    ):
        run_dbt(["run-operation", operation, "--args", run_args])
        run_dbt(["snapshot"])

        records = self._results(project)
        expected_records = {(i, True) for i in current_records}.union({(i, False) for i in closed_records})
        assert records == expected_records

    def test_revived_records_are_recorded_correctly_in_snapshot(self, project: TestProjInfo):
        run_dbt(["run-operation", "delete_records", "--args", "{range: '16 and 20'}"])
        run_dbt(["snapshot"])

        run_dbt(["run-operation", "insert_records", "--args", "{range: '16 and 18'}"])
        run_dbt(["snapshot"])

        records = self._results(project)
        expected_records = {(i, True) for i in range(1, 19)}.union({(i, False) for i in range(16, 21)})
        assert records == expected_records


class TestSnapshotCheck(SnapshotBase):

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _SNAPSHOT_CHECK}

    def test_column_selection_is_reflected_in_snapshot(self, project: TestProjInfo):
        run_dbt([
            "run-operation", "update_tracked_and_untracked_columns",
            "--args", "{untracked_range: '1 and 13', tracked_range: '8 and 18'}"
        ])
        run_dbt(["snapshot"])

        records = self._results(project)
        expected_records = {(i, True) for i in range(1, 21)}.union({(i, False) for i in range(8, 19)})
        assert records == expected_records
