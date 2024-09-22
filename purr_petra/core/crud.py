"""SQLite database CRUD"""

import tempfile
from typing import Dict, Union, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import insert, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import purr_petra.core.models as models
from purr_petra.core.logger import logger


def get_settings(db: Session) -> Dict[str, Union[models.Settings, List[models.Repo]]]:
    """Select all Settings and all Repos (basically everything in SQLite)

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)

    Returns:
        Dict[str, Union[models.Settings, List[models.Repo]]]: One-line from
        settings table and full list of Repos
    """
    settings = db.query(models.Settings).first()
    repos = db.query(models.Repo).all()
    return {"settings": settings, "repos": repos}


def get_file_depot(db: Session) -> Optional[str]:
    """Fetch the file_depot

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)

    Returns:
        Optional[str]: The file_depot or None, but it should never be None,
        since it gets initialized on launch if not present.
    """
    setting = db.query(models.Settings.file_depot).first()
    return setting.file_depot


def init_file_depot(db: Session) -> None:
    """Called in lifespan method to set file_depot to temp folder

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)
    """
    file_depot = db.query(models.Settings.file_depot).first()
    if file_depot is None:
        temp_loc = tempfile.gettempdir()
        logger.info("(Re)initializing file_depot to:", temp_loc)
        stmt = insert(models.Settings).values(file_depot=temp_loc)
        db.execute(stmt)
        db.commit()


def update_file_depot(db: Session, file_depot: str) -> Optional[str]:
    """Insert or Update file_depot value in settings

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)
        file_depot (str): A new file_depot string

    Returns:
        Optional[str]: The currently saved file_depot string
    """
    # not an upsert since there is no independent id key
    fd_count = db.query(models.Settings.file_depot).count()
    if fd_count == 0:
        stmt = insert(models.Settings).values(file_depot=file_depot)
    else:
        stmt = update(models.Settings).values(file_depot=file_depot)
    db.execute(stmt)
    db.commit()
    logger.info(f"Set file_depot: {file_depot}")
    setting = db.query(models.Settings.file_depot).first()
    return setting.file_depot


def upsert_repos(db: Session, repos: List[models.Repo]) -> List[models.Repo]:
    """Upsert Repos

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)
        repos (List[models.Repo]): A list of Repo objects

    Returns:
        List[models.Repo]: List of updated Repo objects
    """
    logger.info(f"Upserting {len(repos)} repos")
    stmt = sqlite_insert(models.Repo).values(repos)
    update_dict = {c.name: c for c in stmt.excluded if c.name != "id"}
    stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_dict)
    db.execute(stmt, repos)
    db.commit()
    ids = [repo["id"] for repo in repos]
    updated_repos = db.query(models.Repo).filter(models.Repo.id.in_(ids)).all()
    return updated_repos


def get_repos(db: Session) -> List[models.Repo]:
    """Fetch a list of Repos

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)

    Returns:
        List[models.Repo]: List of Repos
    """
    repos = db.query(models.Repo).all()
    return repos


def get_repo_by_id(db: Session, repo_id: str) -> Optional[models.Repo]:
    """Fetch a specific Repo based on its ID

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)
        repo_id (str): A Repo.id string

    Returns:
        models.Repo: The selected Repo with matching ID
    """
    repo = db.query(models.Repo).filter_by(id=repo_id).first()
    return repo


def fetch_repo_ids(db: Session) -> List[str]:
    """Fetch a list of Repo IDs

    Args:
        db (Session): Current SQLAlchemy Session (SQLite)

    Returns:
        List[str]: List of Repo IDs
    """
    repo_ids = db.query(models.Repo.id).all()
    return [repo_id[0] for repo_id in repo_ids]
