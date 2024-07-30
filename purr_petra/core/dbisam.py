"""Convenience method for dealing with DBISAM via ODBC"""

import re
from pathlib import Path
from typing import Any, Dict, List
import pyodbc
from purr_petra.core.logger import logger


DBISAM_DRIVER = "DBISAM 4 ODBC Driver"


def db_exec(conn: dict, sql: str) -> List[Dict[str, Any]] | Exception:
    """Convenience method for using pyodbc and DBISAM with Petra

    Args:
        conn (dict): DBISAM connection parameters.
        sql (str): A single SQL statement to execute on the database.

    Returns:
        List[Dict[str, Any]]: list of dicts representing rows from query result.

    Raises:
        - pyodbc.ProgrammingError: For cases where table(s) might not exist due
        to an unxepected/ancient schema. Schema's >~ 2015 should work.
    """

    try:
        # pylint: disable=c-extension-no-member
        with pyodbc.connect(**conn) as connection:
            # I suspect S&P does not modify this per locale, but you should
            # probably verify the dbisam encoding if dealing with non-US data.
            # DBISAM says Locale = "ANSI Standard"
            connection.setencoding("CP1252")

            with connection.cursor() as cursor:
                cursor.execute(sql)

                return [
                    dict(zip([col[0] for col in cursor.description], row))
                    for row in cursor.fetchall()
                ]

    except pyodbc.ProgrammingError as pe:
        logger.error({"error": pe, "context": conn})
        if re.search(r"Table .* not found", str(pe)):
            return pe
    except pyodbc.Error as e:
        logger.error({"error": e, "context": conn})
        # if "DBISAM Engine Error # 11013" in str(e):
        #     logger.error(f"Access denied to temp table or file: {e}")


def make_conn_params(repo_path: str) -> dict:
    """Assemble Petra-centric connection parameters used by pyodbc

    Args:
        repo_path (str): Path to a Petra project base directory.

    Returns:
        dict: dictionary of DBISAM connection parameters.
    """

    params = {"driver": DBISAM_DRIVER, "catalogname": str(Path(repo_path) / "DB")}
    return params
