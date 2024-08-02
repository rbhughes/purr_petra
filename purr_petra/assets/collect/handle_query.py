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


def add_where_clause(uwi_list: List[str], select: str):
    """Construct the UWI-centric part of a WHERE clause containing UWIs. The
    WHERE clause will start: "WHERE 1=1 " to which we append:
    "AND u_uwi LIKE '0123%' OR u_uwi LIKE '4567'"
    Note that this isn't super-efficientt, but it's a decent compromise to keep
    things simple.

    Args:
        uwi_list (List[str]): List of UWI strings with optional wildcard chars
    """

    col = "u.uwi"
    clause = "WHERE 1=1"
    if uwi_list:
        uwis = [f"{col} LIKE '{uwi}'" for uwi in uwi_list]
        clause += " AND " + " OR ".join(uwis)

    query = select.replace("__pUrRwHeRe__", clause)
    return query


# def collect_and_assemble_docs(args: Dict[str, Any]) -> List[Dict[str, Any]]:
def collect_and_assemble_docs(args: Dict[str, Any]):
    """Execute SQL queries and combine results into JSON documents

    Args:
        args (Dict[str, Any]: Contains connection params, a UWI filter and a
        specific 'recipe' for how to query and merge the results into JSON

    Returns:
        List[Dict[str, Any]]: A list of well-centric documents
    """

    print("############################")
    print(args.keys())
    print(args["uwi_list"])
    print("############################")

    conn = args["conn"]

    recipe = args["recipe"]

    select = add_where_clause(uwi_list=args["uwi_list"], select=recipe["select"])

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

    with pyodbc.connect(**conn) as cn:
        cursor = cn.cursor()
        cursor.execute(select)

        for col in cursor.columns():
            print(col)


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
