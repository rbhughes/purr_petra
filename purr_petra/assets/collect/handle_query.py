"""Petra asset query"""

import json
import warnings
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
import numpy as np
import pyodbc

from purr_petra.core.dbisam import db_exec
from purr_petra.core.database import get_db
from purr_petra.core.crud import get_repo_by_id, get_file_depot
from purr_petra.assets.collect.xformer import formatters
from purr_petra.core.util import async_wrap, import_dict_from_file
from purr_petra.assets.collect.post_process import post_process
from purr_petra.assets.collect.xformer import (
    PURR_WHERE,
    transform_dataframe_to_json,
    standardize_df_columns,
)
from purr_petra.assets.collect.sql_helper import (
    get_column_info,
    make_where_clause,
    create_selectors,
    chunk_ids,
)
from purr_petra.core.logger import logger


# because DBISAM isn't exactly popular...
warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy connectable.*"
)

##############################################################################


def fetch_id_list(conn, id_sql):
    """
    Executes and asset recipe's identifier SQL and returns ids.
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

    if not res:
        logger.info("no ids found")
    elif "keylist" in res[0] and res[0]["keylist"] is not None:
        ids = res[0]["keylist"].split(",")
    elif "key" in res[0] and res[0]["key"] is not None:
        ids = [k["key"] for k in res]
    else:
        logger.info("key or keylist missing; cannot make id list")

    return [int_or_string(i) for i in ids]


def collect_and_assemble_docs(args: Dict[str, Any]):
    """todo"""
    conn_params = args["conn"]
    recipe = args["recipe"]
    out_file = args["out_file"]
    xforms = recipe["xforms"]

    # control memory usage by the number of "ids" in the where clause
    chunk_size = recipe["chunk_size"] if "chunk_size" in recipe else 1000

    where = make_where_clause(args["uwi_list"])

    id_sql = recipe["identifier"].replace(PURR_WHERE, where)

    logger.debug(id_sql)

    ids = fetch_id_list(conn_params, id_sql)

    logger.debug(ids)

    chunked_ids = chunk_ids(ids, chunk_size)

    if len(chunked_ids) == 0:
        msg = "Query returned zero hits"
        logger.info(msg)
        return msg

    selectors = create_selectors(chunked_ids, recipe)

    all_columns = set()

    docs_written = 0

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("[")  # Start of JSON array

        for q in selectors:
            logger.debug(q)

            # pylint: disable=c-extension-no-member
            with pyodbc.connect(**conn_params) as conn:
                cursor = conn.cursor()
                cursor.execute(q)

                column_names, column_types = get_column_info(cursor)

                df = pd.DataFrame(
                    [tuple(row) for row in cursor.fetchall()], columns=column_names
                )

                # useful for diagnostics:
                # duplicates = df[df.duplicated(subset=["w_uwi"])]

                df = standardize_df_columns(df, column_types)

                if not df.empty:
                    all_columns.update(df.columns)

                    for col in df.columns:
                        col_type = str(df.dtypes[col])

                        xform = xforms.get(col, col_type)

                        formatter = formatters.get(xform, lambda x: x)

                        # pylint: disable=cell-var-from-loop
                        df[col] = df[col].apply(formatter)

                    df = df.replace({np.nan: None})

                    if postproc := recipe.get("post_process"):
                        post_processor = post_process[postproc]
                        if post_processor:
                            logger.info(f"post-processing: {postproc}")
                            df = post_processor(df)

                    # transform this chunk by table prefixes
                    json_data = transform_dataframe_to_json(df, recipe["prefixes"])

                    logger.info(f"assembled {len(json_data)} docs")

                    for json_obj in json_data:
                        json_str = json.dumps(json_obj, default=str)
                        f.write(json_str + ",")
                        docs_written += 1

        f.seek(f.tell() - 1, 0)  # Remove the last comma
        f.write("]")

    end_msg = f"json docs written: {docs_written}"
    logger.info(end_msg)
    return {"message": end_msg, "out_file": out_file}


async def selector(
    repo_id: str, asset: str, export_file: str, uwi_list: List[str]
) -> str:
    """Main entry point to collect data from a Petra project

    Args:
        repo_id (str): ID from a specific project
        asset (str): An asset (i.e. datatype) to query from project database
        export_file (str): Export file name with timestamp
        uwi_list (str): List of UWI strings

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

    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # with open(result["out_file"], "r") as file:
    #     data = json.load(file)
    #     print(json.dumps(data, indent=2))
    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

    return result
