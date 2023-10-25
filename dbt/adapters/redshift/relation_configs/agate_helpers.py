import agate


def get_first_row(table: agate.Table) -> agate.Row:
    """
    Returns the first row of the table. If the table is empty, it returns an empty row.
    """
    try:
        return table.rows[0]
    except IndexError:
        return agate.Row(values=set())
