"""Misc Utility methods"""

import asyncio
import functools
import hashlib
import json
import socket
import time
import importlib.util
import pyodbc
from datetime import datetime, date
from functools import wraps, partial
from pathlib import Path
from typing import Callable, Dict, List, Union, Optional, Coroutine, Any
import pandas as pd
import numpy as np
from purr_petra.core.logger import logger


def async_wrap(func: callable) -> Callable[..., Coroutine[Any, Any, Any]]:
    """Decorator to allow running a synchronous function in a separate thread.

    Args:
        func (callable: The synchronous function

    Returns:
        callable: A new asynchronous function that runs the original
        synchronous function in an executor.
    """

    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


def is_valid_dir(fs_path: str) -> Optional[str]:
    """Validates and returns path as string if it exists

    Args:
        fs_path (str): A string path that may exist

    Returns:
        Optional[str]: Return Path string if it exists or None if not
    """
    path = Path(fs_path).resolve()
    if path.is_dir():
        return str(path)
    else:
        return None


def generate_repo_id(fs_path: str) -> str:
    """Construct a name + hash id: three chars from name + short hash

    Repos resolved via UNC path vs. drive letter will get different IDs.
    This is intentional.

    Examples:
        //scarab/petra_projects/blank_us_nad27_mean ~~> "BLA_0F0588"

    Args:
        fs_path (str): Full path to a repo (project) directory.

    Returns:
        str: Short, unique id that is easier for humans to work with than UUID.
        Pad with "_" if resulting name length is fewer than 3 chars.
    """
    fp = Path(fs_path)
    prefix = fp.name.upper()[:3]
    if len(fp.name) < 3:
        prefix = prefix.ljust(3, "_")
    suffix = hashlib.md5(str(fp).lower().encode()).hexdigest()[:6]
    return f"{prefix}_{suffix}".upper()


def hostname():
    """Get the hostname where this code is running

    Returns:
        str: PC hostname to lowercase
    """
    return socket.gethostname().lower()


# def hashify(value: Union[str, bytes]) -> str:
#     if isinstance(value, str):
#         value = value.lower().encode("utf-8")
#     if isinstance(value, bytes):
#         value = value.decode("utf-8")
#     uuid_obj = uuid.uuid5(uuid.NAMESPACE_OID, value)
#     return str(uuid_obj)


########################


class CustomJSONEncoder(json.JSONEncoder):
    """
    A sanitizer for data coming from Petra projects.
    Handles Windows CP1252 encoding and various data types.
    """

    def default(
        self, o: Any
    ) -> Union[str, int, float, bool, None, List[Any], Dict[str, Any]]:
        """Encode various data types to JSON-compatible formats."""
        if isinstance(o, (str, int, bool, type(None))):
            return o
        elif isinstance(o, (pd.Timestamp, datetime, date)):
            return o.isoformat()
        elif isinstance(o, (np.integer, np.floating, float)):
            return self._handle_numeric(o)
        elif isinstance(o, np.ndarray):
            return [self.default(x) for x in o.tolist()]
        elif isinstance(o, (np.bool_, bool)):
            return bool(o)
        elif pd.api.types.is_scalar(o):
            return None if pd.isna(o) else o
        elif isinstance(o, (dict, list)):
            return self._handle_container(o)
        elif isinstance(o, pd.Series):
            return self.default(o.tolist())
        elif isinstance(o, pd.DataFrame):
            return self.default(o.to_dict(orient="records"))
        else:
            return self._handle_unknown(o)

    def _handle_numeric(self, obj):
        """Handle numeric types, including NaN and Inf."""
        if np.isnan(obj) or np.isinf(obj):
            return None
        return int(obj) if isinstance(obj, np.integer) else float(obj)

    def _handle_container(self, obj):
        """Handle dict and list containers."""
        if isinstance(obj, dict):
            return {k: self.default(v) for k, v in obj.items()}
        return [self.default(v) for v in obj]

    def _handle_unknown(self, obj):
        """Handle unknown types."""
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

    def encode(self, o: Any) -> str:
        """Encode object to JSON string, converting NaN to null."""
        return json.dumps(self._nan_to_null(self.default(o)), indent=4)

    def _nan_to_null(self, obj):
        """Convert NaN to None recursively."""
        if isinstance(obj, float) and np.isnan(obj):
            return None
        elif isinstance(obj, dict):
            return {k: self._nan_to_null(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._nan_to_null(v) for v in obj]
        return obj


def datetime_formatter(
    format_string="%Y-%m-%dT%H:%M:%S",
) -> Callable[[Union[pd.Timestamp, datetime, date, str, None]], Optional[str]]:
    """Formatter to generate dates like 2022-02-02T12:12:12

    Args:
        format_string (str, optional): Defaults to "%Y-%m-%dT%H:%M:%S".
    """

    def format_datetime(
        x: Union[pd.Timestamp, datetime, date, str, None],
    ) -> Optional[str]:
        if pd.isna(x) or x == "":
            return None
        if isinstance(x, (pd.Timestamp, datetime, date)):
            return x.strftime(format_string)
        return x

    return format_datetime


def timestamp_filename(repo_id: str, asset: str, ext: str = "json"):
    """Simple file name generator for JSON exports

    Examples:
        <repo_id> _ <timestamp> _ <asset> . json
        nor_bd29a9_1721144912_well.json

    Args:
        repo_id (str): The repo_id (three-letter + hash)
        asset (str): The specific (enum) data type from which this data came.
        ext (Optional[str]): file extension--json for now

    Returns:
        str: A plausibly unique export file name
    """
    return f"{repo_id}_{int(time.time())}_{asset}.{ext}".lower()


def debugger(func: Callable[..., Any]) -> Callable[..., Any]:
    """A decorator that can log just about everything. Probably overkill.

    https://ankitbko.github.io/blog/2021/04/logging-in-python/

    Args:
        func: anything (except a generator, I think)

    Returns:
        the wrapped function
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"DEBUGGER [{func.__name__}] w/args: {signature}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"DEBUGGER [{func.__name__}] result: {result}")
            return result
        except Exception as e:
            logger.exception(
                f"DEBUGGER Exception raised [{func.__name__}]. exception: {str(e)}"
            )
            raise e

    return wrapper


def import_dict_from_file(file_path, dict_name):
    """TODO: stuff"""
    spec = importlib.util.spec_from_file_location("module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, dict_name)
