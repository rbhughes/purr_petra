"""Main entry for Repo Recon"""

import asyncio
from pathlib import Path
from typing import Dict, List, Any
from purr_petra.core.crud import upsert_repos
from purr_petra.core.database import get_db
from purr_petra.core.dbisam import make_conn_params
from purr_petra.core.util import generate_repo_id
from purr_petra.recon.epsg import epsg_codes
from purr_petra.recon.repo_db import well_counts, get_polygon, check_dbisam
from purr_petra.recon.repo_fs import network_repo_scan, dir_stats, repo_mod
from purr_petra.core.schemas import Repo


async def repo_recon(recon_root: str) -> List[Dict[str, Any]]:
    """Recursively crawl a network path for Petra project metadata.

    1. repo_paths: identify potential repos by file structure
    2. create initial repo_base dict for each potential repo
    3. repo_list: check DBISAM connectivity, reject (but log) failures
    4. define and run 'augment' functions to add metadata to each repo_base
    5. validate dict against pydantic Repo schema and save to sqlite
    6. reformat repo.repo_mod to string to permit json serialization

    Args:
        recon_root (str): A directory containing Petra repos (projects).

    Returns:
        List[dict]: List of repo dicts containing metadata
    """
    repo_paths = await network_repo_scan(recon_root)
    repo_list = [create_repo_base(rp) for rp in repo_paths]

    # make another pass to verify db
    repo_list = [repo_base for repo_base in repo_list if check_dbisam(repo_base)]

    augment_funcs = [well_counts, get_polygon, epsg_codes, dir_stats, repo_mod]

    async def update_repo(repo_base):
        """Could not use memory tables in SQL if using async_wrap without
        getting DBISAM Engine Error # 11013. I think it's because DBISAM lets
        Windows deal with file locking so connection/cursor closing in pyodbc
        wasn't happening in the thread context.
        """
        for func in augment_funcs:
            # repo_base.update(await async_wrap(func)(repo_base))
            repo_base.update(func(repo_base))
        return repo_base

    repos = await asyncio.gather(*[update_repo(repo) for repo in repo_list])

    valid_repo_dicts = [Repo(**r).model_dump() for r in repos]

    db = next(get_db())
    upsert_repos(db, valid_repo_dicts)
    db.close()

    for r in valid_repo_dicts:
        r["repo_mod"] = r["repo_mod"].strftime("%Y-%m-%d %H:%M:%S")

    return valid_repo_dicts


def create_repo_base(rp: str) -> Dict[str, Any]:
    """
    See repo_recon for details
    """
    return {
        "id": generate_repo_id(rp),
        "active": True,
        "name": Path(rp).name,
        "fs_path": str(Path(rp)),
        "conn": make_conn_params(rp),
        "suite": "petra",
    }
