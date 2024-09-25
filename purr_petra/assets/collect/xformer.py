"""Petra xformers"""

import re
import struct
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List, Union, TypeAlias
import pandas as pd
import numpy as np


PURR_NULL = "_purrNULL_"
PURR_DELIM = "_purrDELIM_"
PURR_WHERE = "_purrWHERE_"

################################################################################


def decode_string(buffer, start, end):
    """Decode a null-terminated string"""
    return buffer[start:end].decode().split("\x00")[0]


def unpack_double(buffer, start):
    """Unpack a double (8 bytes)"""
    return struct.unpack("<d", buffer[start : start + 8])[0]


def unpack_short(buffer, start):
    """Unpack a short(2 bytes)"""
    return struct.unpack("<h", buffer[start : start + 2])[0]


def unpack_int(buffer, start):
    """Unpack an integer (4 bytes)"""
    return struct.unpack("<i", buffer[start : start + 4])[0]


################################################################################


def standardize_df_columns(df: pd.DataFrame, column_types: Dict[str, str]):
    """todo"""
    for col, col_type in column_types.items():
        if "int" in col_type:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
            df[col] = df[col].astype("Int64")
        elif "str" in col_type:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
            df[col] = df[col].astype("string")
        else:
            df[col] = df[col].astype(col_type)
    return df


################################################################################


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


def safe_float(x: Optional[Any]) -> Optional[float]:
    """Convert input to a float or None"""
    if x is None or pd.isna(x):
        return None
    try:
        result = float(x)
        return None if pd.isna(result) else result
    except (ValueError, TypeError, OverflowError):
        return None


def safe_int(x: Optional[Any]) -> Optional[int]:
    """Convert input to an int or None"""
    if x is None:
        return None
    try:
        result = int(x)
        return result
    except (ValueError, TypeError, OverflowError):
        return None


def memo_to_string(x) -> Optional[str]:
    """Strip control chars from DBISAM memo, return str or None"""
    if x is None:
        return None
    if str(x) == "<NA>":  # probably pandas._libs.missing.NAType'
        return None
    return re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))


def blob_to_hex(x) -> Optional[str]:
    """Just return a hex string (for json serialization) or None"""
    if x is None:
        return None
    return f"0x{x.hex()}"


def excel_date(x) -> Optional[str]:
    """Convert Petra's weird (excel?) date float to ISO date string"""
    if x is None:
        return None
    if re.match(r"1[eE]\+?30", str(x), re.IGNORECASE):
        return None
    try:
        return (datetime(1970, 1, 1) + timedelta(days=float(x) - 25569)).isoformat()
    except (ValueError, TypeError):
        return None


def logdata_digits(x) -> Optional[np.ndarray]:
    """Unpack log curve digits from a bytes"""
    if x is None or len(x) == 0:
        return None
    arr = np.frombuffer(x, dtype=np.float64)
    if np.any(~np.isfinite(arr)):
        raise ValueError("Input contains non-finite values")

    return arr


def loglas_lashdr(x) -> Optional[str]:
    """decode the LAS header"""
    if x is None or len(x) == 0:
        return None

    b = [re.sub(r'^"|"$', "", r) for r in bytes(x).decode("utf-8").split(";")]
    return "\n".join(b)


CongressStuff: TypeAlias = Optional[Union[str, float, int]]


def parse_congressional(
    x: Optional[bytes],
) -> Optional[Dict[str, CongressStuff]]:
    """parse binary congressional data to a dict"""
    if x is None:
        return None

    return {
        "township": decode_string(x, 4, 6),
        "township_ns": decode_string(x, 71, 72),
        "range": decode_string(x, 21, 23),
        "range_ew": decode_string(x, 70, 71),
        "section": decode_string(x, 38, 54),
        "section_suffix": decode_string(x, 54, 70),
        "meridian": decode_string(x, 153, 155),
        "footage_ref": decode_string(x, 137, 152),
        "spot": decode_string(x, 96, 136),
        "footage_call_ns": unpack_double(x, 88),
        "footage_call_ns_ref": unpack_short(x, 76),
        "footage_call_ew": unpack_double(x, 80),
        "footage_call_ew_ref": unpack_short(x, 72),
        "remarks": decode_string(x, 156, 412),
    }


def pdtest_treatment(x: Optional[bytes]) -> Optional[List[Dict[str, Any]]]:
    """parse binary pdtest.treat data to a dict, return in list"""

    def parse_treatment(buffer):
        return {
            "type": decode_string(buffer, 0, 9),
            "top": unpack_double(buffer, 9),
            "base": unpack_double(buffer, 17),
            "amount1": unpack_double(buffer, 25),
            "units1": decode_string(buffer, 61, 65),
            "desc": decode_string(buffer, 68, 89),
            "agent": decode_string(buffer, 89, 96),
            "amount2": unpack_double(buffer, 33),
            "units2": decode_string(buffer, 96, 100),
            "fmbrk": unpack_double(buffer, 41),
            "num_stages": unpack_int(buffer, 57),
            "additive": decode_string(buffer, 103, 110),
            "inj_rate": unpack_double(buffer, 49),
        }

    num_bytes = 110
    treatments = (
        [parse_treatment(x[i : i + num_bytes]) for i in range(0, len(x), num_bytes)]
        if x is not None
        else []
    )
    return treatments


def fmtest_recovery(x: Optional[bytes]) -> List[Dict[str, Union[float, str]]]:
    """parse fmtest.recov data to a dict, return in list"""
    if x is None:
        return []

    def parse_recovery(buffer):
        return {
            "amount": unpack_double(buffer, 0),
            "units": decode_string(buffer, 8, 15),
            "descriptions": decode_string(buffer, 15, 36),
        }

    num_bytes = 36
    recoveries = (
        [parse_recovery(x[i : i + num_bytes]) for i in range(0, len(x), num_bytes)]
        if x is not None
        else []
    )
    return recoveries


def parse_zztops(x: Optional[bytes]) -> List[float]:
    """parse zztops.data as list of floats"""
    if x is None:
        return []
    num_bytes = 28
    return [unpack_double(x, i) for i in range(4, len(x), num_bytes)]


################################################################################


def array_of_int(x: Optional[str]) -> List[Optional[int]]:
    """return List of int (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_int(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_float(x: Optional[str]) -> List[Optional[float]]:
    """return List of float (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_float(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_string(x: Optional[str]) -> List[Optional[str]]:
    """return List of str (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [safe_string(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


def array_of_excel_date(x: Optional[str]) -> List[Optional[str]]:
    """return List of excel_date (or empty)"""
    if x is None or pd.isna(x):
        return []
    return [excel_date(v) if v != PURR_NULL else None for v in x.split(PURR_DELIM)]


###############################################################################


def series_row_to_json(
    row: pd.Series, prefix_mapping: Dict[str, str]
) -> Dict[str, Any]:
    """Convert a pandas Series row to a JSON-like dictionary structure."""
    result: Dict[str, Dict[str, Union[None, int, float, str, List[Any]]]] = {}
    for column, value in row.items():
        if isinstance(value, np.ndarray):
            value = value.tolist()

        # Handle list of numpy arrays
        elif isinstance(value, list):
            value = [
                item.tolist() if isinstance(item, np.ndarray) else item
                for item in value
            ]

        elif not isinstance(value, list):
            if pd.isna(value):
                value = None

        for prefix, table_name in prefix_mapping.items():
            if column.startswith(prefix):
                if table_name not in result:
                    result[table_name] = {}
                result[table_name][column[len(prefix) :]] = value
                break
    return result


def transform_dataframe_to_json(
    df: pd.DataFrame, prefix_mapping: Dict[str, str]
) -> List[Dict[str, Dict[str, Union[None, int, float, str, List[Any]]]]]:
    """Convert a DataFrame to a list of JSON-like dictionary structures."""
    return [series_row_to_json(row, prefix_mapping) for _, row in df.iterrows()]


###############################################################################

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
    "array_of_float": array_of_float,
    "array_of_string": array_of_string,
    "array_of_excel_date": array_of_excel_date,
    "fmtest_recovery": fmtest_recovery,
    "pdtest_treatment": pdtest_treatment,
    "parse_zztops": parse_zztops,
}
