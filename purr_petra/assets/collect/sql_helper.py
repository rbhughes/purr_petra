import pyodbc
from typing import Dict, Union, List, Literal, Tuple, TypeAlias

from purr_petra.assets.collect.xformer import PURR_WHERE


def make_where_clause(uwi_list: List[str]):
    """Construct the UWI-centric part of a WHERE clause containing UWIs. The
    WHERE clause will start: "WHERE 1=1 " to which we append:
    "AND u_uwi LIKE '0123%' OR u_uwi LIKE '4567'"

    Args:
        uwi_list (List[str]): List of UWI strings with optional wildcard chars
    """
    # ASSUMES u.uwi WILL ALWAYS BE THE UWI FILTER
    col = "u.uwi"
    clause = "WHERE 1=1"
    if uwi_list:
        uwis = [f"{col} LIKE '{uwi}'" for uwi in uwi_list]
        clause += " AND " + " OR ".join(uwis)

    return clause


def make_id_in_clauses(identifier_keys: List[str], ids: List[Union[str, int]]) -> str:
    """Generate a SQL WHERE clause for filtering by IDs"""
    clause = "WHERE 1=1 "
    if len(identifier_keys) == 1 and str(ids[0]).replace("'", "").isdigit():
        no_quotes = ",".join(str(i).replace("'", "") for i in ids)
        clause += f"AND {identifier_keys[0]} IN ({no_quotes})"
    else:
        idc = " || '-' || ".join(f"CAST({i} AS VARCHAR(10))" for i in identifier_keys)
        clause += f"AND {idc} IN ({','.join(ids)})"
    return clause


def create_selectors(
    chunked_ids: List[List[Union[str, int]]], recipe: Dict[str, str]
) -> List[str]:
    """Create a list of SQL selectors based on recipe and chunked ids"""
    selectors = []
    for ids in chunked_ids:
        in_clause = make_id_in_clauses(recipe["identifier_keys"], ids)
        select_sql = recipe["selector"].replace(PURR_WHERE, in_clause)
        selectors.append(select_sql)
    return selectors


ColTypes: TypeAlias = Union[
    Literal["string", "int64", "float64", "bool", "datetime64[ns]", "object"], str
]


def map_col_type(sql_type):
    """
    Map SQL data types to pandas data types.
    """
    type_map = {
        int: "int64",
        str: "string",
        float: "float64",
        bool: "bool",
        type(None): "object",
        "datetime64[ns]": "datetime64[ns]",
    }
    return type_map.get(sql_type, "object")


def get_column_info(cursor: pyodbc.Cursor) -> Tuple[List[str], Dict[str, str]]:
    """Return column names, types from pyodbc/SQLAnywhere"""
    cursor_desc = cursor.description
    column_names = [col[0] for col in cursor_desc]
    column_types = {col[0]: map_col_type(col[1]) for col in cursor_desc}
    return column_names, column_types


def chunk_ids(ids, chunk):
    """
    [621, 826, 831, 834, 835, 838, 846, 847, 848]
    ...with chunk=4...
    [[621, 826, 831, 834], [835, 838, 846, 847], [848]]

    ["1-62", "1-82", "2-83", "2-83", "2-83", "2-83", "2-84", "3-84", "4-84"]
    ...with chunk=4...
    [
        ['1-62', '1-82'],
        ['2-83', '2-83', '2-83', '2-83', '2-84'],
        ['3-84', '4-84']
    ]
    Note how the group of 2's is kept together, even if it exceeds chunk=4

    :param ids: This is usually a list of wsn ints: [11, 22, 33, 44] but may
        also be "compound" str : ['1-11', '1-22', '1-33', '2-22', '2-44'].
    :param chunk: The preferred batch size to process in a single query
    :return: List of id lists
    """
    id_groups = {}

    for item in ids:
        left = str(item).split("-", maxsplit=1)[0]
        if left not in id_groups:
            id_groups[left] = []
        id_groups[left].append(item)

    result = []
    current_subarray = []

    for group in id_groups.values():
        if len(current_subarray) + len(group) <= chunk:
            current_subarray.extend(group)
        else:
            result.append(current_subarray)
            current_subarray = group[:]

    if current_subarray:
        result.append(current_subarray)

    return result
