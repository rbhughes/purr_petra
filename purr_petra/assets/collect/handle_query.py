"""Petra asset query"""

import json
import warnings
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
import numpy as np
import pyodbc
import collections.abc

from purr_petra.core.dbisam import db_exec
from purr_petra.core.database import get_db
from purr_petra.core.crud import get_repo_by_id, get_file_depot
from purr_petra.assets.collect.xformer import formatters
from purr_petra.core.util import async_wrap, import_dict_from_file
from purr_petra.core.logger import logger

from purr_petra.assets.collect.post_process import post_process

from purr_petra.assets.collect.xformer import PURR_WHERE


warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy connectable.*"
)


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
        # left = str(item).split("-")[0]
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

    # print("$$$$$$$$$$")
    # print(res)
    # print("$$$$$$$$$$")

    ids = []

    if "keylist" in res[0] and res[0]["keylist"] is not None:
        ids = res[0]["keylist"].split(",")
    elif "key" in res[0] and res[0]["key"] is not None:
        ids = [k["key"] for k in res]
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
def transform_row_to_json(
    row: pd.Series, prefix_mapping: Dict[str, str]
) -> Dict[str, Any]:
    result = {}
    for column, value in row.items():
        if isinstance(value, np.ndarray):
            value = value.tolist()
        elif not isinstance(value, list):
            if pd.isna(value):
                value = None

        # if not isinstance(value, np.ndarray) and not isinstance(value, list):
        #     if pd.isna(value):
        #         value = None

        for prefix, table_name in prefix_mapping.items():
            if column.startswith(prefix):
                if table_name not in result:
                    result[table_name] = {}
                result[table_name][column[len(prefix) :]] = value
                break
    return result


def transform_dataframe_to_json(
    df: pd.DataFrame, prefix_mapping: Dict[str, str]
) -> List[Dict[str, Any]]:
    return [transform_row_to_json(row, prefix_mapping) for _, row in df.iterrows()]


#######################################################################


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


#######################################################################
def collect_and_assemble_docs(args: Dict[str, Any]):
    conn_params = args["conn"]
    recipe = args["recipe"]
    out_file = args["out_file"]
    xforms = recipe["xforms"]

    # control memory by the number of "ids" in the where clause of a selector
    chunk_size = recipe["chunk_size"] if "chunk_size" in recipe else 1000

    where = make_where_clause(args["uwi_list"])

    id_sql = recipe["identifier"].replace(PURR_WHERE, where)

    ids = fetch_id_list(conn_params, id_sql)

    chunked_ids = chunk_ids(ids, chunk_size)

    if len(chunked_ids) == 0:
        return "no hits"

    selectors = []

    for c in chunked_ids:
        in_clause = make_id_in_clauses(recipe["identifier_keys"], c)
        select_sql = recipe["selector"].replace(PURR_WHERE, in_clause)
        selectors.append(select_sql)

    all_columns = set()

    ######
    docs_written = 0
    ######

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("[")  # Start of JSON array

        # first_chunk = True

        # Collect all data and column names
        for q in selectors:
            # print("qqqqqqqqqqqqqqqqqqqqqqqqqqq")
            # print(q)
            # print("qqqqqqqqqqqqqqqqqqqqqqqqqqq")

            # pylint: disable=c-extension-no-member
            with pyodbc.connect(**conn_params) as conn:
                cursor = conn.cursor()
                cursor.execute(q)
                cursor_desc = cursor.description

                column_names = [col[0] for col in cursor_desc]
                column_types = {col[0]: map_col_type(col[1]) for col in cursor_desc}

                data = [tuple(row) for row in cursor.fetchall()]

                df = pd.DataFrame(data, columns=column_names)

                for col, col_type in column_types.items():
                    if "int" in col_type:
                        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
                        df[col] = df[col].astype("Int64")
                    elif "str" in col_type:
                        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
                        df[col] = df[col].astype("string")
                    else:
                        df[col] = df[col].astype(col_type)

                if not df.empty:
                    all_columns.update(df.columns)

                    for col in df.columns:
                        col_type = str(df.dtypes[col])

                        xform = xforms.get(col, col_type)

                        formatter = formatters.get(xform, lambda x: x)

                        # pylint: disable=cell-var-from-loop
                        df[col] = df[col].apply(formatter)

                    df = df.replace({np.nan: None})

                    if asset := recipe.get("post_process"):
                        post_processor = post_process[asset]
                        if post_processor:
                            print("post-processing", asset)
                            df = post_processor(df)

                    # for _, row in df.iterrows():
                    #     print(row.to_dict())

                    # transform this chunk by table prefixes
                    json_data = transform_dataframe_to_json(df, recipe["prefixes"])

                    # # Write JSON data to file
                    # if not first_chunk:
                    #     f.write(",")  # Separate JSON objects with a comma
                    # first_chunk = False

                    # TODO: test efficiency vs memory (maybe just send it all)
                    for json_obj in json_data:
                        json_str = json.dumps(json_obj, default=str)
                        f.write(json_str + ",")
                        docs_written += 1

        f.seek(f.tell() - 1, 0)  # Remove the last comma
        f.write("]")

    # return f"{docs_written} docs written to {out_file}"
    return {
        "message": f"{docs_written} {asset} docs written",
        "out_file": out_file,
    }


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
    file_depot = get_file_depot(db)
    db.close()

    depot_path = Path(file_depot)
    out_file = Path(depot_path / export_file)

    if repo is None:
        return "Query returned no repo"

    conn = repo.conn

    recipe_path = Path(Path(__file__).resolve().parent, f"recipes/{asset}.py")
    recipe = import_dict_from_file(recipe_path, "recipe")

    collection_args = {
        "recipe": recipe,
        "repo_id": repo_id,
        "conn": conn,
        "uwi_list": uwi_list,
        "out_file": out_file,
    }

    async_collect_and_assemble_docs = async_wrap(collect_and_assemble_docs)
    result = await async_collect_and_assemble_docs(collection_args)

    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print(result)
    print("-------------------------------------------------")
    # with open(result["out_file"], "r") as file:
    #     data = json.load(file)
    #     print(json.dumps(data, indent=2))
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")


# Print the JSON data with indentation

# if len(records) > 0:
#     async_export_json = async_wrap(export_json)
#     return await async_export_json(records, export_file)
# else:
#     return "Query returned no results"
