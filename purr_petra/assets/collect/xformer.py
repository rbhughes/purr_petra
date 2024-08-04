import re
import struct

import pandas as pd
from datetime import datetime, timedelta

from typing import Any, Dict, Optional, List, Generator


def safe_string(x: Optional[str]) -> str:
    """remove control, non-printable chars, ensure UTF-8, strip whitespace."""
    if x is None:
        return ""
    cleaned = re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))
    try:
        utf8_string = cleaned.encode("latin1").decode("utf-8")
    except UnicodeEncodeError:
        # If the string is already in UTF-8, use the cleaned version
        utf8_string = cleaned
    return "".join(char for char in utf8_string if char.isprintable()).strip()


def memo_to_string(x):
    """strip control chars from DBISAM memo"""
    if x is None:
        return None
    return re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(x))


def blob_to_hex(x):
    """just return a hex (for json serialization)"""
    if x is None:
        return None
    return x.hex()


def safe_numeric(x):
    """try to make a pandas-friendly numeric from int, float, etc."""
    if pd.isna(x) or x == "":
        return None
    try:
        result = pd.to_numeric(x, errors="coerce")
        return None if pd.isna(result) else result
    except (ValueError, TypeError, OverflowError):
        return None


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


def parse_congressional(x: Optional[bytes]) -> Optional[Dict[str, Optional[str]]]:
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


formatters = {
    "int": safe_numeric,
    "float": safe_numeric,
    "str": safe_string,
    "excel_date": excel_date,
    "memo_to_string": memo_to_string,
    "blob_to_hex": blob_to_hex,
    "parse_congressional": parse_congressional,
    "bytearray": lambda x: print(f"was a bytearray {x}"),
}
