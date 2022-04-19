from dbt.tests.util import AnyStringWith, AnyFloat, AnyString


def redshift_stats():
    return {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": True,
            "description": "Indicates whether there are statistics for this table",
            "include": False,
        },
        "encoded": {
            "id": "encoded",
            "label": "Encoded",
            "value": AnyStringWith("Y"),
            "description": "Indicates whether any column in the table has compression encoding defined.",
            "include": True,
        },
        "diststyle": {
            "id": "diststyle",
            "label": "Dist Style",
            "value": AnyStringWith("AUTO"),
            "description": "Distribution style or distribution key column, if key distribution is defined.",
            "include": True,
        },
        "max_varchar": {
            "id": "max_varchar",
            "label": "Max Varchar",
            "value": AnyFloat(),
            "description": "Size of the largest column that uses a VARCHAR data type.",
            "include": True,
        },
        "size": {
            "id": "size",
            "label": "Approximate Size",
            "value": AnyFloat(),
            "description": "Approximate size of the table, calculated from a count of 1MB blocks",
            "include": True,
        },
        "sortkey1": {
            "id": "sortkey1",
            "label": "Sort Key 1",
            "value": AnyString(),
            "description": "First column in the sort key.",
            "include": True,
        },
        "pct_used": {
            "id": "pct_used",
            "label": "Disk Utilization",
            "value": AnyFloat(),
            "description": "Percent of available space that is used by the table.",
            "include": True,
        },
        "stats_off": {
            "id": "stats_off",
            "label": "Stats Off",
            "value": AnyFloat(),
            "description": "Number that indicates how stale the table statistics are; 0 is current, 100 is out of date.",
            "include": True,
        },
        "rows": {
            "id": "rows",
            "label": "Approximate Row Count",
            "value": AnyFloat(),
            "description": "Approximate number of rows in the table. This value includes rows marked for deletion, but not yet vacuumed.",
            "include": True,
        },
    }
