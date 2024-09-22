"""FastAPI Routing for Settings and Repo Recon"""

import asyncio
import json
import uuid
from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
import purr_petra.core.schemas as schemas
import purr_petra.core.crud as crud
from purr_petra.core.database import get_db
from purr_petra.core.util import is_valid_dir

from purr_petra.recon.recon import repo_recon
from purr_petra.core.logger import logger

router = APIRouter()

task_storage: Dict[str, schemas.RepoReconResponse] = {}


async def process_repo_recon(task_id: str, recon_root: str):
    """Trigger repo_recon and update task_storage"""
    try:
        task_storage[task_id].task_status = schemas.TaskStatus.IN_PROGRESS
        repos = await repo_recon(recon_root)
        for r in repos:
            logger.info(json.dumps(r, indent=4))

        task_storage[task_id].task_status = schemas.TaskStatus.COMPLETED
    except Exception as e:  # pylint: disable=broad-except
        task_storage[task_id].task_status = schemas.TaskStatus.FAILED
        logger.error(f"Task failed for {task_id}: {str(e)}")


# FILE_DEPOT ##################################################################


@router.get(
    "/file_depot",
    response_model=schemas.FileDepot,
    summary="Get the directory to store exported JSON files.",
    description="Get file_depot path.",
)
def get_file_depot(db: Session = Depends(get_db)):
    """Get the directory to store exported JSON files"""
    file_depot = crud.get_file_depot(db)
    return {"file_depot": file_depot}


@router.post(
    "/file_depot",
    response_model=schemas.Settings,
    summary="Set the directory to store exported JSON files.",
    description=(
        "The file_depot directory should be accessible to this PC."
        "Data extracted from project databases get written as JSON files here, "
        "each with a unique file name containing source repo and asset type."
    ),
)
def update_file_depot(file_depot: str, db: Session = Depends(get_db)):
    """Set the directory to store exported JSON files"""
    if not is_valid_dir(file_depot):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid directory: {file_depot}",
        )
    try:
        updated_depot = crud.update_file_depot(db=db, file_depot=file_depot)
        return {"file_depot": updated_depot}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while updating the file_depot: {str(e)}",
        ) from e


# REPOS #######################################################################


@router.get(
    "/repos/",
    response_model=list[schemas.Repo],
    summary="Get list of Repos (full)",
    description=(
        "Get a list of all Repos known to the API server. This includes all"
        "metadata and a (potentially) large polygon defining spatial extents."
    ),
)
def read_repos(db: Session = Depends(get_db)):
    """Get list of Repos (full)"""
    repos = crud.get_repos(db)
    return repos


@router.get(
    "/repos/minimal",
    response_model=list[schemas.RepoMinimal],
    summary="Get list of Repos (minimal)",
    description="Get a list of all Repos with minimal metadata",
)
def get_repos(db: Session = Depends(get_db)):
    """Get list of Repos (minimal)"""
    repos = crud.get_repos(db)
    return repos


@router.get(
    "/repos/{repo_id}",
    response_model=schemas.Repo,
    summary="Get a specific Repo by ID",
    description=(
        "Returns all metadata on a single Repo. Use GET /repos/minimal "
        "to see a list Repo IDs along with names and file paths."
    ),
)
def get_repo_by_id(repo_id: str, db: Session = Depends(get_db)):
    """Get a specific Repo by ID"""
    repo = crud.get_repo_by_id(db, repo_id)
    if repo is None:
        raise HTTPException(
            status_code=404,
            detail=f"Repo with id {repo_id} not found",
        )
    return repo


# REPOS RECON #################################################################


@router.post(
    "/repos/recon",
    response_model=schemas.RepoReconResponse,
    summary="Scan network path for Petra projects.",
    description=(
        "Supply a top-level 'recon_root' path to scan for projects (a.k.a. "
        "Repos). Metadata will be collected for valid repos and stored in a "
        "local database. You can collect asset data from these 'known' repos "
        "later. "
        "The task_id is returned immediately; use it to check task status."
    ),
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_repo_recon(recon_root: str):
    """Scan network path for Petra projects"""
    valid_recon_root = is_valid_dir(recon_root)
    if not valid_recon_root:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid directory: {recon_root}",
        )
    task_id = str(uuid.uuid4())
    new_repo_recon = schemas.RepoReconResponse(
        id=task_id,
        recon_root=valid_recon_root,
        task_status=schemas.TaskStatus.PENDING,
    )
    task_storage[task_id] = new_repo_recon

    # do not await create_task, or it will block the 202 response
    # noinspection PyAsyncCall
    asyncio.create_task(process_repo_recon(task_id, valid_recon_root))
    return new_repo_recon


@router.get(
    "/repos/recon/{task_id}",
    response_model=schemas.RepoReconResponse,
    summary="Check the status of a /repos/recon job using the task_id.",
    description=(
        "A recon job may take several minutes, so use the task_id returned "
        "by the original POST to (periodically) check the job status. Possible "
        "status values are: pending, in_progress, completed or failed."
    ),
)
async def get_repo_recon_status(task_id: str):
    """Check the status of a /repos/recon job using the task_id"""
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="repo recon not found")
    return task_storage[task_id]
