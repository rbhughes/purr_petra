"""Stuff involving metadata within Repo databases"""

from purr_petra.core.dbisam import db_exec
from purr_petra.core.logger import logger


def check_dbisam(repo_base) -> bool:
    """A simple query to see if the database WELL table is accessible

    Args:
        repo_base (dict): A stub repo dict.

    Returns:
        bool: True if connection and query were successful, otherwise False
    """
    res = db_exec(repo_base["conn"], "select count(*) from well")
    if isinstance(res, Exception):
        logger.warning(f"Looks like a Petra project but has invalid db?: {res}")
        return False
    elif isinstance(res, list):
        return True
    else:
        return False
