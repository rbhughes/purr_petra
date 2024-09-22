"""Stuff involving metadata within Repo databases"""

from typing import Dict, List, Optional, Tuple
from shapely.geometry import Polygon, MultiPolygon
import numpy as np
import alphashape  # mypy: ignore-missing-imports
from purr_petra.core.dbisam import db_exec
from purr_petra.core.logger import logger

# DBISAM cannot do COUNT(DISTINCT *) and suggests using memory tables as an
# alternative. Watch out for "11013 Access denied" errors.
# (We could use the ODBC result's rowCount as a (horrible) alternative)


NOTNULL_LONLAT = (
    "SELECT s.lon, s.lat FROM well w "
    "LEFT JOIN locat s ON s.wsn = w.wsn "
    "WHERE s.lon IS NOT NULL AND s.lat IS NOT NULL;"
)

WELLS = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Mechanical | Cored Intervals | Any Cores
WELLS_WITH_CORE = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(c.wsn) INTO memory\temp "
    r"FROM cores c JOIN well w ON c.wsn = w.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# NOTE: Regarding DST: within Petra, doing
# [ Select | Wells By Data Criteria | Tests | Any Formation Tests (DST) ]
# yields a slightly higher count. It doesn't use f.testtype = 'D' clause?
WELLS_WITH_DST = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D';"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Geology (Tops) | Tops Data |
# (pick All formations on left)
# When ANY of the Selected Tops Meet the Requirements
# Requirements: If Top is Present in the Database
# ...otherwise you get a bunch of tops with no data.
WELLS_WITH_FORMATION = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM zflddef f "
    r"JOIN zdata z ON f.fid = z.fid "
    r"AND f.kind = 'T' "
    r"AND z.zid = 1 "
    r"AND z.z < 1E30 "
    r"AND z.z IS NOT NULL "
    r"JOIN well w ON z.wsn = w.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Tests | Any Production Tests (IP)
WELLS_WITH_IP = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN pdtest p ON p.wsn = w.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Mechanical | Any Perfs
WELLS_WITH_PERFORATION = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN perfs p ON p.wsn = w.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Production |
# (one-doc-per-mopddef.fid)
WELLS_WITH_PRODUCTION = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN mopddata a ON a.wsn = w.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Logs | Raster Logs | Calibrated Rasters
# (Find Wells With ANY Rasters)
WELLS_WITH_RASTER_LOG = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN logimage i ON w.wsn = i.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Locations | Wells with Directional Survey
# When ANY Condition is Met
WELLS_WITH_SURVEY = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN dirsurvdata d ON d.wsn = w.wsn "
    r"JOIN dirsurvdef f ON f.survrecid = d.survrecid "
    r"GROUP BY w.wsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Logs | Digtial Logs | Log Curves
# Any Curves At All
WELLS_WITH_VECTOR_LOG = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM well w "
    r"JOIN logdata a ON w.wsn = a.wsn "
    r"JOIN logdef f ON a.lsn = f.lsn "
    r"JOIN logdatax x ON a.wsn = x.wsn AND a.lsn = x.lsn AND a.ldsn = x.ldsn;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)

# Select | Wells By Data Criteria | Zones | Zone or Tops Data
# When ANY Condition Is Met...and then this is comparable* to picking every
# Zone and Item individually
# * Probably. I tested a few, but it's too much of a pain.
WELLS_WITH_ZONE = (
    r"DROP TABLE IF EXISTS memory\temp;"
    r"SELECT DISTINCT(w.wsn) INTO memory\temp FROM WELL w "
    r"JOIN zdata z ON z.wsn = w.wsn "
    r"JOIN zonedef n ON n.zid = z.zid AND n.kind > 2 "
    r"JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid;"
    r"SELECT COUNT(*) AS tally from memory\temp;"
)


def check_dbisam(repo_base) -> bool:
    """A simple query to see if the database WELL table is accessible. This will
    cause a DBISAM error if the tables have not been updated to v4.

    Args:
        repo_base (dict): A stub repo dict.

    Returns:
        bool: True if connection and query were successful, otherwise False
    """

    check_sql = "SELECT COUNT(*) AS check FROM well"

    try:
        res = db_exec(repo_base["conn"], check_sql)
        if not isinstance(res, list):
            logger.warning(f"Weirdly broken Petra project?: {res}")
            return False
        return True
    except Exception as e:  # pylint: disable=broad-except
        # logger.error(f"{e}, context: {repo_base["conn"]} {check_sql}")
        logger.error(f"{e}, context: {repo_base}")
        return False


def well_counts(repo_base) -> dict:
    """Run the SQL counts (above) for each asset data type.

    If DBISAM returns an exception the count = None for that asset type

    Args:
        repo_base (dict): A stub repo dict. We just use the fs_path

    Returns:
        A dict with each count, named after the keys below
    """
    logger.info(f"well_counts: {repo_base['fs_path']}")

    counter_sql = {
        "well_count": WELLS,
        # #"wells_with_completion": WELLS_WITH_COMPLETION,
        "wells_with_core": WELLS_WITH_CORE,
        "wells_with_dst": WELLS_WITH_DST,
        "wells_with_formation": WELLS_WITH_FORMATION,
        "wells_with_ip": WELLS_WITH_IP,
        "wells_with_perforation": WELLS_WITH_PERFORATION,
        "wells_with_production": WELLS_WITH_PRODUCTION,
        "wells_with_raster_log": WELLS_WITH_RASTER_LOG,
        "wells_with_survey": WELLS_WITH_SURVEY,
        "wells_with_vector_log": WELLS_WITH_VECTOR_LOG,
        "wells_with_zone": WELLS_WITH_ZONE,
    }

    counts: Dict[str, Optional[int]] = {}

    for key, sql in counter_sql.items():
        res = db_exec(repo_base["conn"], sql)

        if isinstance(res, Exception):
            logger.error({"context": repo_base["fs_path"], "error": res})
            counts[key] = None
        else:
            counts[key] = res[0]["tally"] or 0
    return counts


def concave_hull(points, alpha=0.5) -> Optional[List[Tuple[float, float]]]:
    """Computes a concave hull of a set of points using alpha shape.

    Args:
        points (list): A list of (lon, lat) surface well locations.
        alpha (float): A parameter that controls the concaveness of the hull.
            Higher values of alpha produce more concave hulls.

    Returns:
        list: A list of (lon, lat) tuples representing the vertices of the hull.
    """
    points_array = np.array(points)
    alpha_shape = alphashape.alphashape(points_array, alpha)

    if alpha_shape.is_empty:
        return None
    else:
        if isinstance(alpha_shape, Polygon):
            return [
                (float(coord[0]), float(coord[1]))
                for coord in alpha_shape.exterior.coords
            ]
        elif isinstance(alpha_shape, MultiPolygon):
            # exterior of the largest polygon in MultiPolygon
            largest_polygon = max(alpha_shape.geoms, key=lambda p: p.area)
            return [
                (float(coord[0]), float(coord[1]))
                for coord in largest_polygon.exterior.coords
            ]
        else:
            logger.warning(f"Unexpected geometry type: {type(alpha_shape)}")
            return None


def get_polygon(repo_base) -> Dict[str, Optional[List[Tuple[float, float]]]]:
    """Get a list of lat/lon points defining approximate project boundaries.

    I had used concave_hull (https://concave-hull.readthedocs.io/en/latest/),
    but it relies on numpy 1.26.4. The latest numpy (2.0.0) breaks it.

    This is (obviously) datum agnostic.

    The numpy + alphashape concave_hull function above was cobbled together with
    help from Claude and Perplexity. It's not as fancy, but seems faster and
    has a side effect ot excluding (most) crazy out-of-bounds surface locs.

    Args:
        repo_base (dict): A stub repo dict. We just use the fs_path

    Returns:
        dict with hull (List of points)
    """

    logger.info(f"get_polygon: {repo_base['fs_path']}")

    res = db_exec(repo_base["conn"], NOTNULL_LONLAT)

    if isinstance(res, Exception):
        logger.error({"context": repo_base["fs_path"], "error": res})
        return {"polygon": None}

    points = [[r["lon"], r["lat"]] for r in res]

    if len(points) < 3:
        logger.warning(
            {
                "context": repo_base["fs_path"],
                "message": f"Too few valid Lon/Lat for hull: {repo_base["name"]}",
            }
        )
        return {"polygon": None}

    hull = concave_hull(points)
    if hull is None:
        # a weird edge case I've only seen in very old <2015 vintage projects
        logger.error(
            {
                "context": repo_base["fs_path"],
                "error": "The concave_hull was null, suggesting pre-SQLA17",
            }
        )
        return {"polygon": None}

    first_point = hull[0]
    hull.append(first_point)

    return {"polygon": hull}
