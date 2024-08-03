"""Petra asset query overview.

Asset queries are well-centric. A parent well object is returned with every
asset type. Actual asset data is returned in 'rollups' of child records.
Sometimes this makes perfect sense, but occasionally a one-to-one
relationship is represented as a single-item array.

Why? Ideally, we would specify* exact columns and create more accurate joins
for each asset. However, schemas changes have resulted in new tables and
columns over the years, and that will likely continue. We take the pragmatic
approach and just join on UWI.

* Previous iterations of this utility used fully specified queries; contact
me if you want more details.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Generator
import pyodbc

from purr_petra.core.dbisam import db_exec

import pandas as pd
from purr_petra.core.database import get_db
from purr_petra.core.crud import get_repo_by_id, get_file_depot
from purr_petra.assets.collect.select_recipes import recipes
from purr_petra.core.util import (
    async_wrap,
    datetime_formatter,
    safe_numeric,
    CustomJSONEncoder,
)
from purr_petra.core.logger import logger


formatters = {
    "date": datetime_formatter(),
    "float": safe_numeric,
    "int": safe_numeric,
    "hex": lambda x: (
        x.hex() if isinstance(x, bytes) else (None if pd.isna(x) else str(x))
    ),
    "str": lambda x: str(x) if pd.notna(x) else "",
}


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
        left = str(item).split("-")[0]
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


def fetch_id_list(conn, id_sql):
    """
    :return: Results will be either be a single "keylist"
    [{keylist: ["1-62", "1-82", "2-83", "2-84"]}]
    or a list of key ids
    [{key: "1-62"}, {key: "1-82"}, {key: "2-83"}, {key: "2-84"}]
    Force int() or str(); the typical case is a list of int
    """

    def int_or_string(obj):
        try:
            return int(obj)
        except ValueError:
            return f"'{str(obj).strip()}'"

    res = db_exec(conn, id_sql)

    ids = []
    if "keylist" in res[0]:
        ids = res[0]["keylist"].split(",")
    elif "key" in res[0]:
        ids = [x["key"] for x in res]
    else:
        print("key or keylist missing; cannot make id list")

    return [int_or_string(i) for i in ids]


def make_where_clause(uwi_list: List[str]):
    """Construct the UWI-centric part of a WHERE clause containing UWIs. The
    WHERE clause will start: "WHERE 1=1 " to which we append:
    "AND u_uwi LIKE '0123%' OR u_uwi LIKE '4567'"
    Note that this isn't super-efficientt, but it's a decent compromise to keep
    things simple.

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


def make_id_in_clauses(identifier_keys, ids):
    clause = "WHERE 1=1 "
    if len(identifier_keys) == 1 and str(ids[0]).replace("'", "").isdigit():
        no_quotes = ",".join(str(i).replace("'", "") for i in ids)
        clause += f"AND {identifier_keys[0]} IN ({no_quotes})"
    else:
        idc = " || '-' || ".join(f"CAST({i} AS VARCHAR(10))" for i in identifier_keys)
        clause += f"AND {idc} IN ({','.join(ids)})"
    return clause


#######################################################################
#######################################################################

formatters = {
    "int": safe_numeric,
    "excel_date": lambda x: print(f"you called excel_date with {x}"),
}


#######################################################################
# def collect_and_assemble_docs(args: Dict[str, Any]) -> List[Dict[str, Any]]:
def collect_and_assemble_docs(args: Dict[str, Any]):
    conn = args["conn"]
    recipe = args["recipe"]

    where = make_where_clause(args["uwi_list"])

    id_sql = recipe["identifier"].replace(recipe["purr_where"], where)
    ids = fetch_id_list(conn, id_sql)
    chunked_ids = chunk_ids(ids, 4)

    selectors = []
    for c in chunked_ids:
        in_clause = make_id_in_clauses(recipe["identifier_keys"], c)
        select_sql = recipe["selector"].replace(recipe["purr_where"], in_clause)
        selectors.append(select_sql)

    xforms = recipe["xforms"]

    for q in selectors:
        # pylint: disable=c-extension-no-member
        with pyodbc.connect(**conn) as cn:
            cursor = cn.cursor()
            cursor.execute(q)

            col_mappings = [
                (column[0], column[1].__name__) for column in cursor.description
            ]

            # for col_name, col_type in columns:
            #     print(f"instance Column Name: {col_name}, Data Type: {col_type}")

            for row in cursor.fetchall():
                o = {}
                for i, cell in enumerate(row):
                    key = col_mappings[i][0]
                    dt = col_mappings[i][1]

                    # print(key, dt)
                    # if dt == "int":
                    #     print("int", cell)

                    xform = xforms.get(key, dt)
                    # print(key, "<====>", xform)
                    fr = formatters.get(xform, lambda x: x)(cell)
                    print("------------>", fr)

                    o[key] = cell

                    # print("i", i)
                    # print("cell", cell)
                    # print("mappings[i]", col_mappings[i])
                    # print("----------", col_mappings[i][0])
                # print(o)
                print("--------------------------------")

    ########################################################

    # selector = add_where_clause(uwi_list=args["uwi_list"], sql=recipe["select"])
    # identifier = add_where_clause(uwi_list=args["uwi_list"], sql=recipe["identify"])

    # print("############################")
    # print(ids)
    # print("############################")

    # x = db_exec(conn=conn, sql="select * from well")

    # def get_column_mappings() -> Dict[str, str]:
    #     """Get column names and (odbc-centric) datatypes from pyodbc

    #     Returns:
    #         Dict[str, str]: lower_case column names and datatypes
    #     """
    #     # pylint: disable=c-extension-no-member
    #     with pyodbc.connect(**conn) as cn:
    #         cursor = cn.cursor()
    #         for x in cursor.columns(table="well"):
    #             print(x)

    # get_column_mappings()

    # with pyodbc.connect(**conn) as cn:
    #     cursor = cn.cursor()
    #     cursor.execute(identifier)

    #     for col in cursor.columns():
    #         print(col)


def export_json(records, export_file) -> str:
    """Convert dicts to JSON and save the file.

    Args:
        records (List[Dict[str, Any]]): The list of dicts obtained by
        collect_and_assemble_docs.
        export_file (str): The timestamp export file name defined earlier

    Returns:
        str: A summary containing counts and file path

    TODO: Investigate streaming?
    """
    db = next(get_db())
    file_depot = get_file_depot(db)
    db.close()
    depot_path = Path(file_depot)

    jd = json.dumps(records, indent=4, cls=CustomJSONEncoder)
    out_file = Path(depot_path / export_file)

    with open(out_file, "w", encoding="utf-8") as file:
        file.write(jd)

    return f"Exported {len(records)} docs to: {out_file}"


async def selector(
    repo_id: str, asset: str, export_file: str, uwi_list: List[str]
) -> str:
    """Main entry point to collect data from a GeoGraphix project

    Args:
        repo_id (str): ID from a specific GeoGraphix project
        asset (str): An asset (i.e. datatype) to query from a gxdb
        export_file (str): Export file name with timestamp
        uwi_list (str): A SIMILAR TO clause based on UWI string(s).

    Returns:
        str: A summary of the selector job--probably from export_json()
    """

    db = next(get_db())
    repo = get_repo_by_id(db, repo_id)
    db.close()

    if repo is None:
        return "Query returned no results"

    conn = repo.conn
    print(conn)
    print(asset)

    collection_args = {
        "recipe": recipes[asset],
        "repo_id": repo_id,
        "asset": asset,
        "conn": conn,
        "uwi_list": uwi_list,
    }

    async_collect_and_assemble_docs = async_wrap(collect_and_assemble_docs)
    records = await async_collect_and_assemble_docs(collection_args)

    # if len(records) > 0:
    #     async_export_json = async_wrap(export_json)
    #     return await async_export_json(records, export_file)
    # else:
    #     return "Query returned no results"
