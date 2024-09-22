"""FastAPI Routing for Assets"""

import asyncio
import uuid
from typing import Dict, List, Optional
from enum import Enum
from fastapi import APIRouter, HTTPException, status, Query, Path
from pydantic import BaseModel

from purr_petra.assets.collect.handle_query import selector
from purr_petra.core.database import get_db
from purr_petra.core.crud import fetch_repo_ids
from purr_petra.core.util import timestamp_filename
import purr_petra.core.schemas as schemas
from purr_petra.core.logger import logger


# class CustomJSONResponse(JSONResponse):
#     def render(self, content: any) -> bytes:
#         return json.dumps(
#             content,
#             ensure_ascii=False,
#             allow_nan=False,
#             indent=None,
#             separators=(",", ":"),
#             cls=CustomEncoder,
#         ).encode("utf-8")


class RepoId(BaseModel):
    """Define RepoId class here rather than schemas to avoid extra imports"""

    repo_id: str

    @classmethod
    def validate_repo_id(cls, repo_id):
        """The Repo.id really should exist here"""
        db = next(get_db())
        valid_repo_ids = fetch_repo_ids(db)
        if repo_id not in valid_repo_ids:
            raise HTTPException(status_code=400, detail=f"Invalid repo_id: {repo_id}")
        return repo_id


class AssetTypeEnum(str, Enum):
    """Enums for Asset types"""

    # COMPLETION = "completion"
    CORE = "core"
    DST = "dst"
    FORMATION = "formation"
    IP = "ip"
    PERFORATION = "perforation"
    PRODUCTION = "production"
    RASTER_LOG = "raster_log"
    SURVEY = "survey"
    VECTOR_LOG = "vector_log"
    WELL = "well"
    ZONE = "zone"


def parse_uwis(uwis: Optional[str]) -> List[str]:
    """Parse POSTed uwi string into a suitable SQLAnywhere SIMILAR TO clause.
    Split by commas or spaces, replace '*' with '%', joined to '|'

    Example:
        0505* pilot %0001 -> '0505%|pilot|%0001'

    Args:
        uwis (Optional[str]): incoming request string

    Returns:
        List[str]: A list of UWI strings if present
    """
    if uwis is None:
        return uwis

    try:
        split = [
            item
            for item in uwis.strip().replace(",", " ").replace('"', "").split()
            if item
        ]
        parsed = [item.replace("*", "%").replace("'", "''") for item in split]
        logger.debug(f"parse_uwi returns: {parsed}")
        return parsed
    except AttributeError:
        logger.error(f"'uwis' must be a string, not {type(uwis)}")
        return []
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Unexpected error occurred: {str(e)}")
        return []


router = APIRouter()

task_storage: Dict[str, schemas.AssetCollectionResponse] = {}


async def process_asset_collection(
    task_id: str, repo_id: str, asset: str, export_file: str, uwi_list: str
):
    """Trigger selector and update task_storage"""
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        res = await selector(repo_id, asset, export_file, uwi_list)
        logger.info(res)
        task_storage[task_id].task_message = res
        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
        return res
    except Exception as e:  # pylint: disable=broad-except
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        logger.error(f"Task failed for {task_id}: {str(e)}")


# ASSETS ######################################################################


@router.post(
    "/asset/{repo_id}/{asset}",
    response_model=schemas.AssetCollectionResponse,
    summary="Query a Repo for Asset data",
    description=(
        "Specify a repo_id, asset (data type) and an optional uwi filter. "
        "Query results will be written to files stored in the 'file_depot' "
        "directory."
    ),
    status_code=status.HTTP_202_ACCEPTED,
)
async def asset_collection(
    repo_id: str = Path(..., description="repo_id"),
    asset: AssetTypeEnum = Path(..., description="asset type"),
    uwi_query: str = Query(
        None,
        min_length=3,
        description="Enter full or partial uwi(s); use * or % as wildcard."
        "Separate UWIs with spaces or commas. Leave blank to select all.",
    ),
):
    """Query a Repo for Asset data"""
    RepoId.validate_repo_id(repo_id)
    asset = asset.value

    uwi_list = parse_uwis(uwi_query)

    task_id = str(uuid.uuid4())

    export_file = timestamp_filename(repo_id=repo_id, asset=asset)

    new_collect = schemas.AssetCollectionResponse(
        id=task_id,
        repo_id=repo_id,
        asset=asset,
        uwi_list=uwi_list,
        task_status=schemas.TaskStatus.PENDING,
        task_message=f"export file (pending): {export_file}",
    )
    task_storage[task_id] = new_collect

    # noinspection PyAsyncCall
    asyncio.create_task(
        process_asset_collection(
            task_id,
            repo_id,
            asset,
            export_file,
            uwi_list,
        )
    )
    return new_collect


@router.get(
    "/asset/status/{task_id}",
    response_model=schemas.AssetCollectionResponse,
    summary="Check status of a /asset/{repo_id}/{asset} job using the task_id.",
    description=(
        "An assect collection job may take several minutes, so use the task_id "
        "returned by the original POST to (periodically) check the job status. "
        "Status values are: pending, in_progress, completed or failed. Query "
        "results will be written to the file_depot directory."
    ),
)
async def get_asset_collect_status(task_id: str):
    """Check status of a /asset/{repo_id}/{asset} job using the task_id"""
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="Asset collection task not found")
    return task_storage[task_id]
