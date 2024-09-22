from typing import Dict, Union, List

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
