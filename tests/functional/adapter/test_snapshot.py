from typing import List, Tuple

import pytest
import psycopg2

from dbt.tests.util import run_dbt, relation_from_name
from dbt.tests.fixtures.project import TestProjInfo


_SEED_BASE = """
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
""".strip()


_SEED_INSERT = """
id,first_name,last_name,email,gender,ip_address,updated_at
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


_SNAPSHOT_TIMESTAMP = """
{% snapshot snapshot_timestamp %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
    ) }}
    {% if var('invalidate_hard_deletes', 'false') | as_bool %}
        {{ config(invalidate_hard_deletes=True) }}
    {% endif %}
    select * from {{ ref('seed_base') }}
{% endsnapshot %}
"""


_SNAPSHOT_HARD_DELETE = """
{% snapshot snapshot_hard_delete %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    ) }}
    select * from {{ ref('seed_base') }}
{% endsnapshot %}
"""


_SNAPSHOT_CHECK = """
{% snapshot snapshot_check %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='check',
        check_cols=['email'],
    ) }}
    select * from {{ ref('seed_base') }}
{% endsnapshot %}
"""


_MACRO_UPDATE = """
{% macro updated_records_are_captured_by_snapshot() %}
    {% set sql %}
        update {{ ref('seed_base') }}
        set updated_at = updated_at + interval '1 day',
            email      = left(email, 3)
        where id between 16 and 20
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
"""


class SnapshotBase:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_base.csv": _SEED_BASE,
            "seed_insert.csv": _SEED_INSERT,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_timestamp.sql": _SNAPSHOT_TIMESTAMP,
            "snapshot_hard_delete.sql": _SNAPSHOT_HARD_DELETE,
            "snapshot_check.sql": _SNAPSHOT_CHECK,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "update.sql": _MACRO_UPDATE
        }

    @property
    def _snapshot_names(self) -> List[str]:
        return ["snapshot_timestamp", "snapshot_hard_delete", "snapshot_check"]

    @pytest.fixture(scope="function", autouse=True)
    def _setup(self, project: TestProjInfo):
        run_dbt(["seed", "--full-refresh"])
        self._truncate_snapshots(project)
        run_dbt(["snapshot"])

    def _truncate_snapshots(self, project: TestProjInfo):
        for snapshot in self._snapshot_names:
            try:
                project.run_sql(f"""delete from {relation_from_name(project.adapter, snapshot)}""")
            except psycopg2.errors.UndefinedTable:
                pass  # the snapshot table does not exist before the first test run

    @staticmethod
    def _snapshot_records_with_id_and_is_current(project: TestProjInfo, snapshot: str) -> List[Tuple[str, bool]]:
        return project.run_sql(f"""
            select id, dbt_valid_to is null as is_current
            from {relation_from_name(project.adapter, snapshot)}
        """, fetch="all")


class TestSnapshot(SnapshotBase):

    def test_updated_records_are_captured_by_snapshot(self, project: TestProjInfo):
        """
        - make updates to 5 records and rerun the snapshot
        - show that the snapshot reflects the updates
            - 15 original records remain current
            - 5 original records are closed out
            - 5 new records for the updates which are now current
        """
        run_dbt(["run-operation", "updated_records_are_captured_by_snapshot"])
        run_dbt(["snapshot", "--select", "snapshot_timestamp"])

        records = self._snapshot_records_with_id_and_is_current(project, "snapshot_timestamp")
        expected_records = [(i, True) for i in range(1, 16)] + \
                           [(i, False) for i in range(16, 21)] + \
                           [(i, True) for i in range(16, 21)]
        assert set(records) == set(expected_records)

    def test_inserted_records_are_captured_by_snapshot(self, project: TestProjInfo):
        """
        - insert 10 new records (new ids)
        - show that the snapshot reflects the updates
            - 20 original records remain current
            - 10 new records for the inserts which are now current
        """
        project.run_sql(f"""
            insert into {relation_from_name(project.adapter, "seed_base")}
            select * from {relation_from_name(project.adapter, "seed_insert")}
        """)
        run_dbt(["snapshot", "--select", "snapshot_timestamp"])

        records = self._snapshot_records_with_id_and_is_current(project, "snapshot_timestamp")
        expected_records = [(i, True) for i in range(1, 31)]
        assert set(records) == set(expected_records)

    def test_new_column_in_base_is_recorded_correctly_in_snapshot(self, project: TestProjInfo):
        """
        - add a new column and populate it with a value
        - show that the snapshot reflects the updates
            - 20 original records are closed out
            - 20 new records for the update which are now current
        """
        project.run_sql(f"""
            alter table {relation_from_name(project.adapter, "seed_base")}
            add column full_name varchar(200) default null
        """)
        project.run_sql(f"""
            update {relation_from_name(project.adapter, "seed_base")}
            set full_name = first_name || ' ' || last_name,
                updated_at = updated_at + interval '1 day' 
        """)
        run_dbt(["snapshot", "--select", "snapshot_timestamp"])

        records = self._snapshot_records_with_id_and_is_current(project, "snapshot_timestamp")
        expected_records = [(i, True) for i in range(1, 21)] +\
                           [(i, False) for i in range(1, 21)]
        assert set(records) == set(expected_records)

    def test_hard_delete_closes_out_records_in_snapshot(self, project: TestProjInfo):
        """
        - hard delete 5 records
        - show that the snapshot reflects the deletes
            - 15 original records remain current
            - 5 original records are closed out
        """
        project.run_sql(f"""
            delete from {relation_from_name(project.adapter, "seed_base")}
            where id between 16 and 20
        """)
        run_dbt(["snapshot", "--select", "snapshot_hard_delete"])

        records = self._snapshot_records_with_id_and_is_current(project, "snapshot_hard_delete")
        expected_records = [(i, True) for i in range(1, 16)] +\
                           [(i, False) for i in range(16, 21)]
        assert set(records) == set(expected_records)

    def test_revived_records_are_recorded_correctly_in_snapshot(self, project: TestProjInfo):
        """
        - retain 3 records to be revived (same data and same id)
        - hard delete 5 records (including the 3 to be revived)
        - run snapshot to close out those 5 records
        - insert the 3 retained records
        - show that the snapshot reflects the revives
            - 15 original records remain current
            - 5 original records are closed out
            - 3 new records from the revived records which are now current
        """
        project.run_sql(f"""
            create table {relation_from_name(project.adapter, "seed_temp")} as
            select * from {relation_from_name(project.adapter, "seed_base")}
            where id between 16 and 18
        """)
        project.run_sql(f"""
            delete from {relation_from_name(project.adapter, "seed_base")}
            where id between 16 and 20
        """)
        run_dbt(["snapshot", "--select", "snapshot_hard_delete"])

        project.run_sql(f"""
            insert into {relation_from_name(project.adapter, "seed_base")}
            select * from {relation_from_name(project.adapter, "seed_temp")}
        """)
        run_dbt(["snapshot", "--select", "snapshot_hard_delete"])

        records = self._snapshot_records_with_id_and_is_current(project, "snapshot_hard_delete")
        expected_records = [(i, True) for i in range(1, 16)] + \
                           [(i, False) for i in range(16, 21)] +\
                           [(i, True) for i in range(16, 19)]
        assert set(records) == set(expected_records)

    def test_column_selection_is_reflected_in_snapshot(self, project: TestProjInfo):
        """
        - update 7 records for a column that is not checked by snapshot
        - update 6 records for a column that is not checked by snapshot and another column that is checked by snapshot
        - update 5 records for a column that is checked by snapshot
        - leave 2 records untouched
        - run snapshot
        - show that the snapshot reflects the checked changes
            - 7 original records remain current despite change
            - 11 original records are closed out
            - 11 new records for the updates which are now current
            - 2 original records remain current with no change
        """
        project.run_sql(f"""
            update {relation_from_name(project.adapter, "seed_base")}
            set last_name = left(last_name, 3)
            where id between 1 and 13
        """)
        project.run_sql(f"""
            update {relation_from_name(project.adapter, "seed_base")}
            set email      = left(email, 3)
            where id between 8 and 18
        """)
        run_dbt(["snapshot", "--select", "snapshot_check"])

        records = self._snapshot_records_with_id_and_is_current(project, "snapshot_check")
        expected_records = [(i, True) for i in range(1, 8)] + \
                           [(i, False) for i in range(8, 19)] + \
                           [(i, True) for i in range(8, 19)] + \
                           [(i, True) for i in range(19, 21)]
        assert set(records) == set(expected_records)
