"""SQLAlchemy Model definition"""

from sqlalchemy import Boolean, Column, Integer, String, JSON, TIMESTAMP

# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeBase

# Base = declarative_base()


class Base(DeclarativeBase):
    """SQLAlchemy Base class"""


class Repo(Base):
    """Definition of SQLAlchemy Repo object"""

    __tablename__ = "repos"

    id = Column(String, primary_key=True, index=True, unique=True)
    active = Column(Boolean)
    name = Column(String)
    fs_path = Column(String)
    conn = Column(JSON)
    # conn_aux = Column(JSON)
    suite = Column(String)
    well_count = Column(Integer)
    wells_with_completion = Column(Integer)
    wells_with_core = Column(Integer)
    wells_with_dst = Column(Integer)
    wells_with_formation = Column(Integer)
    wells_with_ip = Column(Integer)
    wells_with_perforation = Column(Integer)
    wells_with_production = Column(Integer)
    wells_with_raster_log = Column(Integer)
    wells_with_survey = Column(Integer)
    wells_with_vector_log = Column(Integer)
    wells_with_zone = Column(Integer)
    storage_epsg = Column(Integer)
    storage_name = Column(String)
    display_epsg = Column(Integer)
    display_name = Column(String)
    files = Column(Integer)
    directories = Column(Integer)
    bytes = Column(Integer)
    repo_mod = Column(TIMESTAMP)
    polygon = Column(JSON)


class Settings(Base):
    """Defintion of SQLAlchemy Settings object"""

    __tablename__ = "settings"

    file_depot = Column(
        String,
        primary_key=True,
        nullable=True,
        server_default="C:/temp",
    )
