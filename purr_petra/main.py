"""Main entry point of purr_petra"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import os
import uvicorn
from purr_petra.core import routes_settings
from purr_petra.assets.collect import routes_assets
from purr_petra.core.crud import init_file_depot
from purr_petra.core.database import get_db
from purr_petra.core.logger import logger
from purr_petra.prep.setup import prepare


@asynccontextmanager
async def lifespan(fastapp: FastAPI):  # pylint: disable=unused-argument
    """https://fastapi.tiangolo.com/advanced/events/

    Args:
        fastapi (FastAPI): FastAPI instance, not actually used in the function
    """
    db = next(get_db())
    init_file_depot(db)
    yield


app = FastAPI(lifespan=lifespan)

app.include_router(routes_settings.router, prefix="/purr/petra")
app.include_router(routes_assets.router, prefix="/purr/petra")

# to generate current openapi schema
# import json
# openapi_schema = app.openapi()
# with open("./docs/openapi.json", "w") as f:
#     json.dump(openapi_schema, f, indent=2)

purr_port = int(os.environ.get("PURR_PETRA_PORT", "8070"))
purr_host = os.environ.get("PURR_PETRA_HOST", "0.0.0.0")
purr_workers = int(os.environ.get("PURR_PETRA_WORKERS", "4"))


def prep():
    logger.info("Installing ODBC and DU utility")
    prepare()


def start():
    logger.info("Initializing purr_petra API (prod)")
    uvicorn.run(
        "purr_petra.main:app",
        host=purr_host,
        port=purr_port,
        workers=purr_workers,
    )


if __name__ == "__main__":
    logger.info("Initializing purr_petra API (dev)")
    uvicorn.run(app, host=purr_host, port=purr_port)

# to run in dev, just do:   python -m purr_petra.main
