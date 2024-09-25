"""Pydantic Schemas"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from pydantic import BaseModel


class SettingsBase(BaseModel):
    """Pydantic model (Base) for Settings"""

    file_depot: Optional[str] = None


class Settings(SettingsBase):
    """Pydantic model for Settings"""

    class Config:
        """Pydantic voodoo"""

        from_attributes = True


class FileDepot(BaseModel):
    """Pydantic model for FileDepot"""

    file_depot: Optional[str] = None


# class Coordinates(BaseModel):
#     coordinates: List[Tuple[float, float]] = Field(
#         ...,
#         description="A list of longitude-latitude pairs",
#         example=[
#             [-97.90777, 27.62867],
#             [-97.91186, 27.62672]
#         ]
#     )


class RepoBase(BaseModel):
    """Pydantic model (Base) for Repo"""

    active: bool
    name: str
    fs_path: str
    conn: Dict[str, Any]
    ### conn_aux: Dict[str, Any] | None
    suite: str
    well_count: int | None
    # wells_with_completion: int | None
    wells_with_core: int | None
    wells_with_dst: int | None
    wells_with_formation: int | None
    wells_with_ip: int | None
    wells_with_perforation: int | None
    wells_with_production: int | None
    wells_with_raster_log: int | None
    wells_with_survey: int | None
    wells_with_vector_log: int | None
    wells_with_zone: int | None
    storage_epsg: int
    storage_name: str
    display_epsg: int
    display_name: str
    files: int
    directories: int
    bytes: int
    repo_mod: datetime
    polygon: List[Tuple[float, float]] | None


class Repo(RepoBase):
    """Pydantic model for Repo"""

    id: str

    class Config:
        """Pydantic voodoo"""

        from_attributes = True


class RepoMinimal(BaseModel):
    """Pydantic model for Minimal Repo"""

    id: str
    name: str
    fs_path: str
    well_count: int

    class Config:
        """Pydantic voodoo"""

        from_attributes = True


class TaskStatus(str, Enum):
    """TaskStatus Enum"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class RepoReconCreate(BaseModel):
    """Pydantic model for RepoReconCreate"""

    recon_root: str


class RepoReconResponse(BaseModel):
    """Pydantic model for RepoReconResponse"""

    id: str
    recon_root: str
    task_status: TaskStatus


class AssetCollectionResponse(BaseModel):
    """Pydantic model for AssetCollectionResponse"""

    id: str
    task_status: TaskStatus
    task_message: str
