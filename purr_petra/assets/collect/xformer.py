"""Petra xformers"""

import re
import struct
import numpy as np

import pandas as pd
from datetime import datetime, timedelta

from typing import Any, Dict, Optional, List, Generator, Union

PURR_NULL = "_purrNULL_"
PURR_DELIM = "_purrDELIM_"
PURR_WHERE = "_purrWHERE_"


def safe_string(x: Optional[str]) -> Optional[str]:
    """remove control, non-printable chars, ensure UTF-8, strip whitespace."""
    if x is None:
        return None
    if str(x) == "<NA>":  # probably pandas._libs.missing.NAType'
        return None
    cleaned = re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))
    try:
        utf8_string = cleaned.encode("latin1").decode("utf-8")
    except UnicodeEncodeError:
        # If the string is already in UTF-8, use the cleaned version
        utf8_string = cleaned
    return "".join(char for char in utf8_string if char.isprintable()).strip()


# def safe_numeric(x, col_type: Optional[str]):
#     """try to make a pandas-friendly numeric from int, float, etc."""
#     if pd.isna(x) or x == "":
#         return None
#     try:
#         result = pd.to_numeric(x, errors="coerce")
#         return None if pd.isna(result) else result
#     except (ValueError, TypeError, OverflowError):
#         return None


def safe_float(x: Optional[float]) -> Optional[float]:
    """Convert input to a float if possible, otherwise return None."""
    if pd.isna(x):
        return None
    try:
        result = float(x)
        return None if pd.isna(result) else result
    except (ValueError, TypeError, OverflowError):
        return None


def safe_int(x: Optional[int]) -> Optional[int]:
    """Convert input to an integer if possible, otherwise return None."""
    if x is None:
        return None
    try:
        result = int(x)
        return result
    except (ValueError, TypeError, OverflowError):
        return None


def memo_to_string(x):
    """strip control chars from DBISAM memo"""
    if x is None:
        return None
    if str(x) == "<NA>":  # probably pandas._libs.missing.NAType'
        return None
    return re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))


def blob_to_hex(x):
    """just return a hex (for json serialization)"""
    # TODO: check if 0x<hex> is valid, probably depends on use case
    if x is None:
        return None
    return f"0x{x.hex()}"


def excel_date(x):
    """convert Petra's weird (excel?) date float to real date"""
    if x is None:
        return None
    if re.match(r"1[eE]\+?30", str(x), re.IGNORECASE):
        return None
    try:
        return (datetime(1970, 1, 1) + timedelta(days=float(x) - 25569)).isoformat()
    except (ValueError, TypeError):
        return None


def logdata_digits(x):
    """Unpack log curve digits from a bytes object.

    Args:
        x (Optional[bytes]): The bytes object containing the log curve digits.

    Returns:
        Optional[np.ndarray]: A NumPy array of unpacked double-precision floats, or None if input is None or empty.
    """
    if x is None or len(x) == 0:
        return None

    arr = np.frombuffer(x, dtype=np.float64)
    if np.any(~np.isfinite(arr)):
        raise ValueError("Input contains non-finite values")

    return arr


def loglas_lashdr(x):
    """decode the LAS header"""
    b = [re.sub(r'^"|"$', "", r) for r in bytes(x).decode("utf-8").split(";")]
    return "\n".join(b)


def parse_congressional(
    x: Optional[bytes],
) -> Optional[Dict[str, Union[Optional[str], Optional[float], Optional[int]]]]:
    """parse the binary congressional data to a dict"""
    if x is None:
        return None

    def decode_field(start: int, end: int) -> Optional[str]:
        try:
            return x[start:end].decode().split("\x00")[0]
        except (IndexError, UnicodeDecodeError):
            return None

    def unpack_double(start: int) -> Optional[float]:
        try:
            return struct.unpack("<d", x[start : start + 8])[0]
        except struct.error:
            return None

    def unpack_short(start: int) -> Optional[int]:
        try:
            return struct.unpack("<h", x[start : start + 2])[0]
        except struct.error:
            return None

    return {
        "township": decode_field(4, 6),
        "township_ns": decode_field(71, 72),
        "range": decode_field(21, 23),
        "range_ew": decode_field(70, 71),
        "section": decode_field(38, 54),
        "section_suffix": decode_field(54, 70),
        "meridian": decode_field(153, 155),
        "footage_ref": decode_field(137, 152),
        "spot": decode_field(96, 136),
        "footage_call_ns": unpack_double(88),
        "footage_call_ns_ref": unpack_short(76),
        "footage_call_ew": unpack_double(80),
        "footage_call_ew_ref": unpack_short(72),
        "remarks": decode_field(156, 412),
    }


def fmtest_recovery(x: Optional[bytes]) -> List[Dict[str, Union[float, str]]]:
    if x is None:
        return []

    def parse_recovery(chunk: bytes):
        amount = struct.unpack("<d", chunk[:8])[0]
        units = chunk[8:15].decode().split("\x00")[0]
        descriptions = chunk[15:36].decode().split("\x00")[0]
        return {"amount": amount, "units": units, "descriptions": descriptions}

    return [parse_recovery(x[i : i + 36]) for i in range(0, len(x), 36)]


def array_of_int(x):
    return [safe_int(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_float(x):
    return [safe_float(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_string(x):
    return [safe_string(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_excel_date(x):
    return [excel_date(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


# def tester(x, col_type):
#     return "TESTER TESTER " + col_type


###############################################################################

# TODO: add bool and datetime64[ns]
formatters = {
    "Int64": safe_int,
    "float64": safe_float,
    "string": safe_string,
    "memo_to_string": memo_to_string,
    "blob_to_hex": blob_to_hex,
    "excel_date": excel_date,
    "logdata_digits": logdata_digits,
    "loglas_lashdr": loglas_lashdr,
    "parse_congressional": parse_congressional,
    "array_of_int": array_of_int,
    "array_of_float": array_of_int,
    "array_of_string": array_of_int,
    "array_of_excel_date": array_of_int,
    "fmtest_recovery": fmtest_recovery,
}
