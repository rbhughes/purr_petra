"""SQLAlchemy configuration (SQLite)"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import purr_petra.core.models as models

SQLALCHEMY_DATABASE_URL = "sqlite:///./purr_petra.sqlite"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

models.Base.metadata.create_all(bind=engine)


def get_db():
    """Generator function for local SQLAlchemy database (SQLite)

    Yields:
        SessionLocal: An instance of a database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
