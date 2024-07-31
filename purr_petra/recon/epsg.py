import re

from purr_petra.core.dbisam import db_exec
from typing import Any, Dict, Union, Optional, List

from purr_petra.core.logger import logger


geodetics: List[Dict[str, Any]] = [
    {"code": 4326, "datum": "wgs84", "name": "wgs84"},
    {"code": 4267, "datum": "nad27", "name": "nad27"},
    {"code": 4269, "datum": "nad83", "name": "nad83"},
    {"code": 4277, "datum": "ord surv gb", "name": "ord surv gb"},
    {"code": 21500, "datum": "belgium", "name": "belgium"},
    {"code": 4301, "datum": "tokyo-japan", "name": "tokyo-japan"},
    {"code": 4284, "datum": "pulkovo 1942", "name": "pulkovo 1942"},
    {"code": 4314, "datum": "dhdn", "name": "dhdn"},
    {"code": 4272, "datum": "geodetic49", "name": "geodetic49"},
]

projections: List[Dict[str, Any]] = [
    {
        "code": 32601,
        "datum": "wgs84",
        "name": "utm-01n",
    },
    {
        "code": 32602,
        "datum": "wgs84",
        "name": "utm-02n",
    },
    {
        "code": 32603,
        "datum": "wgs84",
        "name": "utm-03n",
    },
    {
        "code": 32604,
        "datum": "wgs84",
        "name": "utm-04n",
    },
    {
        "code": 32605,
        "datum": "wgs84",
        "name": "utm-05n",
    },
    {
        "code": 32606,
        "datum": "wgs84",
        "name": "utm-06n",
    },
    {
        "code": 32607,
        "datum": "wgs84",
        "name": "utm-07n",
    },
    {
        "code": 32608,
        "datum": "wgs84",
        "name": "utm-08n",
    },
    {
        "code": 32609,
        "datum": "wgs84",
        "name": "utm-09n",
    },
    {
        "code": 32610,
        "datum": "wgs84",
        "name": "utm-10n",
    },
    {
        "code": 32611,
        "datum": "wgs84",
        "name": "utm-11n",
    },
    {
        "code": 32612,
        "datum": "wgs84",
        "name": "utm-12n",
    },
    {
        "code": 32613,
        "datum": "wgs84",
        "name": "utm-13n",
    },
    {
        "code": 32614,
        "datum": "wgs84",
        "name": "utm-14n",
    },
    {
        "code": 32615,
        "datum": "wgs84",
        "name": "utm-15n",
    },
    {
        "code": 32616,
        "datum": "wgs84",
        "name": "utm-16n",
    },
    {
        "code": 32617,
        "datum": "wgs84",
        "name": "utm-17n",
    },
    {
        "code": 32618,
        "datum": "wgs84",
        "name": "utm-18n",
    },
    {
        "code": 32619,
        "datum": "wgs84",
        "name": "utm-19n",
    },
    {
        "code": 32620,
        "datum": "wgs84",
        "name": "utm-20n",
    },
    {
        "code": 32621,
        "datum": "wgs84",
        "name": "utm-21n",
    },
    {
        "code": 32622,
        "datum": "wgs84",
        "name": "utm-22n",
    },
    {
        "code": 32623,
        "datum": "wgs84",
        "name": "utm-23n",
    },
    {
        "code": 32624,
        "datum": "wgs84",
        "name": "utm-24n",
    },
    {
        "code": 32625,
        "datum": "wgs84",
        "name": "utm-25n",
    },
    {
        "code": 32626,
        "datum": "wgs84",
        "name": "utm-26n",
    },
    {
        "code": 32627,
        "datum": "wgs84",
        "name": "utm-27n",
    },
    {
        "code": 32628,
        "datum": "wgs84",
        "name": "utm-28n",
    },
    {
        "code": 32629,
        "datum": "wgs84",
        "name": "utm-29n",
    },
    {
        "code": 32630,
        "datum": "wgs84",
        "name": "utm-30n",
    },
    {
        "code": 32631,
        "datum": "wgs84",
        "name": "utm-31n",
    },
    {
        "code": 32632,
        "datum": "wgs84",
        "name": "utm-32n",
    },
    {
        "code": 32633,
        "datum": "wgs84",
        "name": "utm-33n",
    },
    {
        "code": 32634,
        "datum": "wgs84",
        "name": "utm-34n",
    },
    {
        "code": 32635,
        "datum": "wgs84",
        "name": "utm-35n",
    },
    {
        "code": 32636,
        "datum": "wgs84",
        "name": "utm-36n",
    },
    {
        "code": 32637,
        "datum": "wgs84",
        "name": "utm-37n",
    },
    {
        "code": 32638,
        "datum": "wgs84",
        "name": "utm-38n",
    },
    {
        "code": 32639,
        "datum": "wgs84",
        "name": "utm-39n",
    },
    {
        "code": 32640,
        "datum": "wgs84",
        "name": "utm-40n",
    },
    {
        "code": 32641,
        "datum": "wgs84",
        "name": "utm-41n",
    },
    {
        "code": 32642,
        "datum": "wgs84",
        "name": "utm-42n",
    },
    {
        "code": 32643,
        "datum": "wgs84",
        "name": "utm-43n",
    },
    {
        "code": 32644,
        "datum": "wgs84",
        "name": "utm-44n",
    },
    {
        "code": 32645,
        "datum": "wgs84",
        "name": "utm-45n",
    },
    {
        "code": 32646,
        "datum": "wgs84",
        "name": "utm-46n",
    },
    {
        "code": 32647,
        "datum": "wgs84",
        "name": "utm-47n",
    },
    {
        "code": 32648,
        "datum": "wgs84",
        "name": "utm-48n",
    },
    {
        "code": 32649,
        "datum": "wgs84",
        "name": "utm-49n",
    },
    {
        "code": 32650,
        "datum": "wgs84",
        "name": "utm-50n",
    },
    {
        "code": 32651,
        "datum": "wgs84",
        "name": "utm-51n",
    },
    {
        "code": 32652,
        "datum": "wgs84",
        "name": "utm-52n",
    },
    {
        "code": 32653,
        "datum": "wgs84",
        "name": "utm-53n",
    },
    {
        "code": 32654,
        "datum": "wgs84",
        "name": "utm-54n",
    },
    {
        "code": 32655,
        "datum": "wgs84",
        "name": "utm-55n",
    },
    {
        "code": 32656,
        "datum": "wgs84",
        "name": "utm-56n",
    },
    {
        "code": 32657,
        "datum": "wgs84",
        "name": "utm-57n",
    },
    {
        "code": 32658,
        "datum": "wgs84",
        "name": "utm-58n",
    },
    {
        "code": 32659,
        "datum": "wgs84",
        "name": "utm-59n",
    },
    {
        "code": 32660,
        "datum": "wgs84",
        "name": "utm-60n",
    },
    {
        "code": 32701,
        "datum": "wgs84",
        "name": "utm-01s",
    },
    {
        "code": 32702,
        "datum": "wgs84",
        "name": "utm-02s",
    },
    {
        "code": 32703,
        "datum": "wgs84",
        "name": "utm-03s",
    },
    {
        "code": 32704,
        "datum": "wgs84",
        "name": "utm-04s",
    },
    {
        "code": 32705,
        "datum": "wgs84",
        "name": "utm-05s",
    },
    {
        "code": 32706,
        "datum": "wgs84",
        "name": "utm-06s",
    },
    {
        "code": 32707,
        "datum": "wgs84",
        "name": "utm-07s",
    },
    {
        "code": 32708,
        "datum": "wgs84",
        "name": "utm-08s",
    },
    {
        "code": 32709,
        "datum": "wgs84",
        "name": "utm-09s",
    },
    {
        "code": 32710,
        "datum": "wgs84",
        "name": "utm-10s",
    },
    {
        "code": 32711,
        "datum": "wgs84",
        "name": "utm-11s",
    },
    {
        "code": 32712,
        "datum": "wgs84",
        "name": "utm-12s",
    },
    {
        "code": 32713,
        "datum": "wgs84",
        "name": "utm-13s",
    },
    {
        "code": 32714,
        "datum": "wgs84",
        "name": "utm-14s",
    },
    {
        "code": 32715,
        "datum": "wgs84",
        "name": "utm-15s",
    },
    {
        "code": 32716,
        "datum": "wgs84",
        "name": "utm-16s",
    },
    {
        "code": 32717,
        "datum": "wgs84",
        "name": "utm-17s",
    },
    {
        "code": 32718,
        "datum": "wgs84",
        "name": "utm-18s",
    },
    {
        "code": 32719,
        "datum": "wgs84",
        "name": "utm-19s",
    },
    {
        "code": 32720,
        "datum": "wgs84",
        "name": "utm-20s",
    },
    {
        "code": 32721,
        "datum": "wgs84",
        "name": "utm-21s",
    },
    {
        "code": 32722,
        "datum": "wgs84",
        "name": "utm-22s",
    },
    {
        "code": 32723,
        "datum": "wgs84",
        "name": "utm-23s",
    },
    {
        "code": 32724,
        "datum": "wgs84",
        "name": "utm-24s",
    },
    {
        "code": 32725,
        "datum": "wgs84",
        "name": "utm-25s",
    },
    {
        "code": 32726,
        "datum": "wgs84",
        "name": "utm-26s",
    },
    {
        "code": 32727,
        "datum": "wgs84",
        "name": "utm-27s",
    },
    {
        "code": 32728,
        "datum": "wgs84",
        "name": "utm-28s",
    },
    {
        "code": 32729,
        "datum": "wgs84",
        "name": "utm-29s",
    },
    {
        "code": 32730,
        "datum": "wgs84",
        "name": "utm-30s",
    },
    {
        "code": 32731,
        "datum": "wgs84",
        "name": "utm-31s",
    },
    {
        "code": 32732,
        "datum": "wgs84",
        "name": "utm-32s",
    },
    {
        "code": 32733,
        "datum": "wgs84",
        "name": "utm-33s",
    },
    {
        "code": 32734,
        "datum": "wgs84",
        "name": "utm-34s",
    },
    {
        "code": 32735,
        "datum": "wgs84",
        "name": "utm-35s",
    },
    {
        "code": 32736,
        "datum": "wgs84",
        "name": "utm-36s",
    },
    {
        "code": 32737,
        "datum": "wgs84",
        "name": "utm-37s",
    },
    {
        "code": 32738,
        "datum": "wgs84",
        "name": "utm-38s",
    },
    {
        "code": 32739,
        "datum": "wgs84",
        "name": "utm-39s",
    },
    {
        "code": 32740,
        "datum": "wgs84",
        "name": "utm-40s",
    },
    {
        "code": 32741,
        "datum": "wgs84",
        "name": "utm-41s",
    },
    {
        "code": 32742,
        "datum": "wgs84",
        "name": "utm-42s",
    },
    {
        "code": 32743,
        "datum": "wgs84",
        "name": "utm-43s",
    },
    {
        "code": 32744,
        "datum": "wgs84",
        "name": "utm-44s",
    },
    {
        "code": 32745,
        "datum": "wgs84",
        "name": "utm-45s",
    },
    {
        "code": 32746,
        "datum": "wgs84",
        "name": "utm-46s",
    },
    {
        "code": 32747,
        "datum": "wgs84",
        "name": "utm-47s",
    },
    {
        "code": 32748,
        "datum": "wgs84",
        "name": "utm-48s",
    },
    {
        "code": 32749,
        "datum": "wgs84",
        "name": "utm-49s",
    },
    {
        "code": 32750,
        "datum": "wgs84",
        "name": "utm-50s",
    },
    {
        "code": 32751,
        "datum": "wgs84",
        "name": "utm-51s",
    },
    {
        "code": 32752,
        "datum": "wgs84",
        "name": "utm-52s",
    },
    {
        "code": 32753,
        "datum": "wgs84",
        "name": "utm-53s",
    },
    {
        "code": 32754,
        "datum": "wgs84",
        "name": "utm-54s",
    },
    {
        "code": 32755,
        "datum": "wgs84",
        "name": "utm-55s",
    },
    {
        "code": 32756,
        "datum": "wgs84",
        "name": "utm-56s",
    },
    {
        "code": 32757,
        "datum": "wgs84",
        "name": "utm-57s",
    },
    {
        "code": 32758,
        "datum": "wgs84",
        "name": "utm-58s",
    },
    {
        "code": 32759,
        "datum": "wgs84",
        "name": "utm-59s",
    },
    {
        "code": 32760,
        "datum": "wgs84",
        "name": "utm-60s",
    },
    {
        "code": 26729,
        "datum": "nad27",
        "name": "al-27e",
    },
    {
        "code": 26730,
        "datum": "nad27",
        "name": "al-27w",
    },
    {
        "code": 26731,
        "datum": "nad27",
        "name": "ak1-27",
    },
    {
        "code": 26732,
        "datum": "nad27",
        "name": "ak2-27",
    },
    {
        "code": 26733,
        "datum": "nad27",
        "name": "ak3-27",
    },
    {
        "code": 26734,
        "datum": "nad27",
        "name": "ak4-27",
    },
    {
        "code": 26735,
        "datum": "nad27",
        "name": "ak5-27",
    },
    {
        "code": 26736,
        "datum": "nad27",
        "name": "ak6-27",
    },
    {
        "code": 26737,
        "datum": "nad27",
        "name": "ak7-27",
    },
    {
        "code": 26738,
        "datum": "nad27",
        "name": "ak8-27",
    },
    {
        "code": 26739,
        "datum": "nad27",
        "name": "ak9-27",
    },
    {
        "code": 26740,
        "datum": "nad27",
        "name": "ak10-27",
    },
    {
        "code": 26748,
        "datum": "nad27",
        "name": "az-27e",
    },
    {
        "code": 26749,
        "datum": "nad27",
        "name": "az-27c",
    },
    {
        "code": 26750,
        "datum": "nad27",
        "name": "az-27w",
    },
    {
        "code": 26751,
        "datum": "nad27",
        "name": "ar-27n",
    },
    {
        "code": 26752,
        "datum": "nad27",
        "name": "ar-27s",
    },
    {
        "code": 26741,
        "datum": "nad27",
        "name": "ca1-27",
    },
    {
        "code": 26742,
        "datum": "nad27",
        "name": "ca2-27",
    },
    {
        "code": 26743,
        "datum": "nad27",
        "name": "ca3-27",
    },
    {
        "code": 26744,
        "datum": "nad27",
        "name": "ca4-27",
    },
    {
        "code": 26745,
        "datum": "nad27",
        "name": "ca5-27",
    },
    {
        "code": 26746,
        "datum": "nad27",
        "name": "ca6-27",
    },
    {
        "code": 26747,
        "datum": "nad27",
        "name": "ca7-27",
    },
    {
        "code": 26753,
        "datum": "nad27",
        "name": "co-27n",
    },
    {
        "code": 26754,
        "datum": "nad27",
        "name": "co-27c",
    },
    {
        "code": 26755,
        "datum": "nad27",
        "name": "co-27s",
    },
    {
        "code": 26756,
        "datum": "nad27",
        "name": "ct-27",
    },
    {
        "code": 26757,
        "datum": "nad27",
        "name": "de-27",
    },
    {
        "code": 26758,
        "datum": "nad27",
        "name": "fl-27n",
    },
    {
        "code": 26759,
        "datum": "nad27",
        "name": "fl-27e",
    },
    {
        "code": 26760,
        "datum": "nad27",
        "name": "fl-27w",
    },
    {
        "code": 26766,
        "datum": "nad27",
        "name": "ga-27e",
    },
    {
        "code": 26767,
        "datum": "nad27",
        "name": "ga-27w",
    },
    {
        "code": 26761,
        "datum": "nad27",
        "name": "hi1-27",
    },
    {
        "code": 26762,
        "datum": "nad27",
        "name": "hi2-27",
    },
    {
        "code": 26763,
        "datum": "nad27",
        "name": "hi3-27",
    },
    {
        "code": 26764,
        "datum": "nad27",
        "name": "hi4-27",
    },
    {
        "code": 26765,
        "datum": "nad27",
        "name": "hi5-27",
    },
    {
        "code": 26768,
        "datum": "nad27",
        "name": "id-27e",
    },
    {
        "code": 26769,
        "datum": "nad27",
        "name": "id-27c",
    },
    {
        "code": 26770,
        "datum": "nad27",
        "name": "id-27w",
    },
    {
        "code": 26771,
        "datum": "nad27",
        "name": "il-27e",
    },
    {
        "code": 26772,
        "datum": "nad27",
        "name": "il-27w",
    },
    {
        "code": 26773,
        "datum": "nad27",
        "name": "ia-27e",
    },
    {
        "code": 26774,
        "datum": "nad27",
        "name": "ia-27w",
    },
    {
        "code": 26775,
        "datum": "nad27",
        "name": "io-27n",
    },
    {
        "code": 26776,
        "datum": "nad27",
        "name": "io-27s",
    },
    {
        "code": 26777,
        "datum": "nad27",
        "name": "ks-27n",
    },
    {
        "code": 26778,
        "datum": "nad27",
        "name": "ks-27s",
    },
    {
        "code": 26779,
        "datum": "nad27",
        "name": "ky-27n",
    },
    {
        "code": 26780,
        "datum": "nad27",
        "name": "ky-27s",
    },
    {
        "code": 26781,
        "datum": "nad27",
        "name": "la-27n",
    },
    {
        "code": 26782,
        "datum": "nad27",
        "name": "la-27s",
    },
    {
        "code": 26783,
        "datum": "nad27",
        "name": "me-27e",
    },
    {
        "code": 26784,
        "datum": "nad27",
        "name": "me-27w",
    },
    {
        "code": 26785,
        "datum": "nad27",
        "name": "md-27",
    },
    {
        "code": 26786,
        "datum": "nad27",
        "name": "ma-27m",
    },
    {
        "code": 26787,
        "datum": "nad27",
        "name": "ma-27i",
    },
    {
        "code": 26788,
        "datum": "nad27",
        "name": "mi-27n",
    },
    {
        "code": 26789,
        "datum": "nad27",
        "name": "mi-27c",
    },
    {
        "code": 26790,
        "datum": "nad27",
        "name": "mi-27s",
    },
    {
        "code": 26791,
        "datum": "nad27",
        "name": "mn-27n",
    },
    {
        "code": 26792,
        "datum": "nad27",
        "name": "mn-27c",
    },
    {
        "code": 26793,
        "datum": "nad27",
        "name": "mn-27s",
    },
    {
        "code": 26794,
        "datum": "nad27",
        "name": "ms-27e",
    },
    {
        "code": 26795,
        "datum": "nad27",
        "name": "ms-27w",
    },
    {
        "code": 26796,
        "datum": "nad27",
        "name": "mo-27e",
    },
    {
        "code": 26797,
        "datum": "nad27",
        "name": "mo-27c",
    },
    {
        "code": 26798,
        "datum": "nad27",
        "name": "mo-27w",
    },
    {
        "code": 32001,
        "datum": "nad27",
        "name": "mt-27n",
    },
    {
        "code": 32002,
        "datum": "nad27",
        "name": "mt-27c",
    },
    {
        "code": 32003,
        "datum": "nad27",
        "name": "mt-27s",
    },
    {
        "code": 32005,
        "datum": "nad27",
        "name": "ne-27n",
    },
    {
        "code": 32006,
        "datum": "nad27",
        "name": "ne-27s",
    },
    {
        "code": 32007,
        "datum": "nad27",
        "name": "nv-27e",
    },
    {
        "code": 32008,
        "datum": "nad27",
        "name": "nv-27c",
    },
    {
        "code": 32009,
        "datum": "nad27",
        "name": "nv-27w",
    },
    {
        "code": 32010,
        "datum": "nad27",
        "name": "nh-27",
    },
    {
        "code": 32011,
        "datum": "nad27",
        "name": "nj-27",
    },
    {
        "code": 32012,
        "datum": "nad27",
        "name": "nm-27e",
    },
    {
        "code": 32013,
        "datum": "nad27",
        "name": "nm-27c",
    },
    {
        "code": 32014,
        "datum": "nad27",
        "name": "nm-27w",
    },
    {
        "code": 32015,
        "datum": "nad27",
        "name": "ny-27e",
    },
    {
        "code": 32016,
        "datum": "nad27",
        "name": "ny-27c",
    },
    {
        "code": 32017,
        "datum": "nad27",
        "name": "ny-27w",
    },
    {
        "code": 32018,
        "datum": "nad27",
        "name": "ny-27i",
    },
    {
        "code": 32019,
        "datum": "nad27",
        "name": "nc-27",
    },
    {
        "code": 32020,
        "datum": "nad27",
        "name": "nd-27n",
    },
    {
        "code": 32021,
        "datum": "nad27",
        "name": "nd-27s",
    },
    {
        "code": 32022,
        "datum": "nad27",
        "name": "oh-27n",
    },
    {
        "code": 32023,
        "datum": "nad27",
        "name": "oh-27s",
    },
    {
        "code": 32024,
        "datum": "nad27",
        "name": "ok-27n",
    },
    {
        "code": 32025,
        "datum": "nad27",
        "name": "ok-27s",
    },
    {
        "code": 32026,
        "datum": "nad27",
        "name": "or-27n",
    },
    {
        "code": 32027,
        "datum": "nad27",
        "name": "or-27s",
    },
    {
        "code": 32028,
        "datum": "nad27",
        "name": "pa-27n",
    },
    {
        "code": 32029,
        "datum": "nad27",
        "name": "pa-27s",
    },
    {
        "code": 32059,
        "datum": "nad27",
        "name": "prvi-27",
    },
    {
        "code": 32030,
        "datum": "nad27",
        "name": "ri-27",
    },
    {
        "code": 32031,
        "datum": "nad27",
        "name": "sc-27n",
    },
    {
        "code": 32033,
        "datum": "nad27",
        "name": "sc-27s",
    },
    {
        "code": 32034,
        "datum": "nad27",
        "name": "sd-27n",
    },
    {
        "code": 32035,
        "datum": "nad27",
        "name": "sd-27s",
    },
    {
        "code": 32036,
        "datum": "nad27",
        "name": "tn-27",
    },
    {
        "code": 32037,
        "datum": "nad27",
        "name": "tx-27n",
    },
    {
        "code": 32038,
        "datum": "nad27",
        "name": "tx-27nc",
    },
    {
        "code": 32039,
        "datum": "nad27",
        "name": "tx-27c",
    },
    {
        "code": 32040,
        "datum": "nad27",
        "name": "tx-27sc",
    },
    {
        "code": 32041,
        "datum": "nad27",
        "name": "tx-27s",
    },
    {
        "code": 32042,
        "datum": "nad27",
        "name": "ut-27n",
    },
    {
        "code": 32043,
        "datum": "nad27",
        "name": "ut-27c",
    },
    {
        "code": 32044,
        "datum": "nad27",
        "name": "ut-27s",
    },
    {
        "code": 32045,
        "datum": "nad27",
        "name": "vt-27",
    },
    {
        "code": 32046,
        "datum": "nad27",
        "name": "va-27n",
    },
    {
        "code": 32047,
        "datum": "nad27",
        "name": "va-27s",
    },
    {
        "code": 32048,
        "datum": "nad27",
        "name": "wa-27n",
    },
    {
        "code": 32049,
        "datum": "nad27",
        "name": "wa-27s",
    },
    {
        "code": 32050,
        "datum": "nad27",
        "name": "wv-27n",
    },
    {
        "code": 32051,
        "datum": "nad27",
        "name": "wv-27s",
    },
    {
        "code": 32052,
        "datum": "nad27",
        "name": "wi-27n",
    },
    {
        "code": 32053,
        "datum": "nad27",
        "name": "wi-27c",
    },
    {
        "code": 32054,
        "datum": "nad27",
        "name": "wi-27s",
    },
    {
        "code": 32055,
        "datum": "nad27",
        "name": "wy-27e",
    },
    {
        "code": 32056,
        "datum": "nad27",
        "name": "wy-27ec",
    },
    {
        "code": 32057,
        "datum": "nad27",
        "name": "wy-27wc",
    },
    {
        "code": 32058,
        "datum": "nad27",
        "name": "wy-27w",
    },
    {
        "code": 26729,
        "datum": "nad27",
        "name": "al-27e",
    },
    {
        "code": 26730,
        "datum": "nad27",
        "name": "al-27w",
    },
    {
        "code": 26731,
        "datum": "nad27",
        "name": "ak1-27",
    },
    {
        "code": 26732,
        "datum": "nad27",
        "name": "ak2-27",
    },
    {
        "code": 26733,
        "datum": "nad27",
        "name": "ak3-27",
    },
    {
        "code": 26734,
        "datum": "nad27",
        "name": "ak4-27",
    },
    {
        "code": 26735,
        "datum": "nad27",
        "name": "ak5-27",
    },
    {
        "code": 26736,
        "datum": "nad27",
        "name": "ak6-27",
    },
    {
        "code": 26737,
        "datum": "nad27",
        "name": "ak7-27",
    },
    {
        "code": 26738,
        "datum": "nad27",
        "name": "ak8-27",
    },
    {
        "code": 26739,
        "datum": "nad27",
        "name": "ak9-27",
    },
    {
        "code": 26740,
        "datum": "nad27",
        "name": "ak10-27",
    },
    {
        "code": 26748,
        "datum": "nad27",
        "name": "az-27e",
    },
    {
        "code": 26749,
        "datum": "nad27",
        "name": "az-27c",
    },
    {
        "code": 26750,
        "datum": "nad27",
        "name": "az-27w",
    },
    {
        "code": 26751,
        "datum": "nad27",
        "name": "ar-27n",
    },
    {
        "code": 26752,
        "datum": "nad27",
        "name": "ar-27s",
    },
    {
        "code": 26741,
        "datum": "nad27",
        "name": "ca1-27",
    },
    {
        "code": 26742,
        "datum": "nad27",
        "name": "ca2-27",
    },
    {
        "code": 26743,
        "datum": "nad27",
        "name": "ca3-27",
    },
    {
        "code": 26744,
        "datum": "nad27",
        "name": "ca4-27",
    },
    {
        "code": 26745,
        "datum": "nad27",
        "name": "ca5-27",
    },
    {
        "code": 26746,
        "datum": "nad27",
        "name": "ca6-27",
    },
    {
        "code": 26747,
        "datum": "nad27",
        "name": "ca7-27",
    },
    {
        "code": 26753,
        "datum": "nad27",
        "name": "co-27n",
    },
    {
        "code": 26754,
        "datum": "nad27",
        "name": "co-27c",
    },
    {
        "code": 26755,
        "datum": "nad27",
        "name": "co-27s",
    },
    {
        "code": 26756,
        "datum": "nad27",
        "name": "ct-27",
    },
    {
        "code": 26757,
        "datum": "nad27",
        "name": "de-27",
    },
    {
        "code": 26758,
        "datum": "nad27",
        "name": "fl-27e",
    },
    {
        "code": 26759,
        "datum": "nad27",
        "name": "fl-27w",
    },
    {
        "code": 26760,
        "datum": "nad27",
        "name": "fl-27n",
    },
    {
        "code": 26766,
        "datum": "nad27",
        "name": "ga-27e",
    },
    {
        "code": 26767,
        "datum": "nad27",
        "name": "ga-27w",
    },
    {
        "code": 26761,
        "datum": "nad27",
        "name": "hi1-27",
    },
    {
        "code": 26762,
        "datum": "nad27",
        "name": "hi2-27",
    },
    {
        "code": 26763,
        "datum": "nad27",
        "name": "hi3-27",
    },
    {
        "code": 26764,
        "datum": "nad27",
        "name": "hi4-27",
    },
    {
        "code": 26765,
        "datum": "nad27",
        "name": "hi5-27",
    },
    {
        "code": 26768,
        "datum": "nad27",
        "name": "id-27e",
    },
    {
        "code": 26769,
        "datum": "nad27",
        "name": "id-27c",
    },
    {
        "code": 26770,
        "datum": "nad27",
        "name": "id-27w",
    },
    {
        "code": 26771,
        "datum": "nad27",
        "name": "il-27e",
    },
    {
        "code": 26772,
        "datum": "nad27",
        "name": "il-27w",
    },
    {
        "code": 26774,
        "datum": "nad27",
        "name": "ia-27w",
    },
    {
        "code": 26773,
        "datum": "nad27",
        "name": "ia-27e",
    },
    {
        "code": 26775,
        "datum": "nad27",
        "name": "io-27n",
    },
    {
        "code": 26776,
        "datum": "nad27",
        "name": "io-27s",
    },
    {
        "code": 26777,
        "datum": "nad27",
        "name": "ks-27n",
    },
    {
        "code": 26778,
        "datum": "nad27",
        "name": "ks-27s",
    },
    {
        "code": 26779,
        "datum": "nad27",
        "name": "ky-27n",
    },
    {
        "code": 26780,
        "datum": "nad27",
        "name": "ky-27s",
    },
    {
        "code": 26781,
        "datum": "nad27",
        "name": "la-27n",
    },
    {
        "code": 26782,
        "datum": "nad27",
        "name": "la-27s",
    },
    {
        "code": 26783,
        "datum": "nad27",
        "name": "me-27e",
    },
    {
        "code": 26784,
        "datum": "nad27",
        "name": "me-27w",
    },
    {
        "code": 26785,
        "datum": "nad27",
        "name": "md-27",
    },
    {
        "code": 26786,
        "datum": "nad27",
        "name": "ma-27m",
    },
    {
        "code": 26787,
        "datum": "nad27",
        "name": "ma-27i",
    },
    {
        "code": 26788,
        "datum": "nad27",
        "name": "mi-27n",
    },
    {
        "code": 26789,
        "datum": "nad27",
        "name": "mi-27c",
    },
    {
        "code": 26790,
        "datum": "nad27",
        "name": "mi-27s",
    },
    {
        "code": 26801,
        "datum": "nad27",
        "name": "mi-27e",
    },
    {
        "code": 26802,
        "datum": "nad27",
        "name": "mi-27c",
    },
    {
        "code": 26803,
        "datum": "nad27",
        "name": "mi-27w",
    },
    {
        "code": 26791,
        "datum": "nad27",
        "name": "mn-27n",
    },
    {
        "code": 26792,
        "datum": "nad27",
        "name": "mn-27c",
    },
    {
        "code": 26793,
        "datum": "nad27",
        "name": "mn-27s",
    },
    {
        "code": 26794,
        "datum": "nad27",
        "name": "ms-27e",
    },
    {
        "code": 26795,
        "datum": "nad27",
        "name": "ms-27w",
    },
    {
        "code": 26796,
        "datum": "nad27",
        "name": "mo-27e",
    },
    {
        "code": 26797,
        "datum": "nad27",
        "name": "mo-27c",
    },
    {
        "code": 26798,
        "datum": "nad27",
        "name": "mo-27w",
    },
    {
        "code": 32001,
        "datum": "nad27",
        "name": "mt-27n",
    },
    {
        "code": 32002,
        "datum": "nad27",
        "name": "mt-27e",
    },
    {
        "code": 32003,
        "datum": "nad27",
        "name": "mt-27s",
    },
    {
        "code": 32005,
        "datum": "nad27",
        "name": "ne-27n",
    },
    {
        "code": 32006,
        "datum": "nad27",
        "name": "ne-27s",
    },
    {
        "code": 32007,
        "datum": "nad27",
        "name": "nv-27e",
    },
    {
        "code": 32008,
        "datum": "nad27",
        "name": "nv-27c",
    },
    {
        "code": 32009,
        "datum": "nad27",
        "name": "nv-27w",
    },
    {
        "code": 32010,
        "datum": "nad27",
        "name": "nh-27",
    },
    {
        "code": 32011,
        "datum": "nad27",
        "name": "nj-27",
    },
    {
        "code": 32012,
        "datum": "nad27",
        "name": "nm-27e",
    },
    {
        "code": 32013,
        "datum": "nad27",
        "name": "nm-27c",
    },
    {
        "code": 32014,
        "datum": "nad27",
        "name": "nm-27w",
    },
    {
        "code": 32018,
        "datum": "nad27",
        "name": "ny-27i",
    },
    {
        "code": 32015,
        "datum": "nad27",
        "name": "ny-27e",
    },
    {
        "code": 32016,
        "datum": "nad27",
        "name": "ny-27c",
    },
    {
        "code": 32017,
        "datum": "nad27",
        "name": "ny-27w",
    },
    {
        "code": 32019,
        "datum": "nad27",
        "name": "nc-27",
    },
    {
        "code": 32020,
        "datum": "nad27",
        "name": "nd-27n",
    },
    {
        "code": 32021,
        "datum": "nad27",
        "name": "nd-27s",
    },
    {
        "code": 32022,
        "datum": "nad27",
        "name": "oh-27n",
    },
    {
        "code": 32023,
        "datum": "nad27",
        "name": "oh-27s",
    },
    {
        "code": 32024,
        "datum": "nad27",
        "name": "ok-27n",
    },
    {
        "code": 32025,
        "datum": "nad27",
        "name": "ok-27s",
    },
    {
        "code": 32026,
        "datum": "nad27",
        "name": "or-27n",
    },
    {
        "code": 32027,
        "datum": "nad27",
        "name": "or-27s",
    },
    {
        "code": 32028,
        "datum": "nad27",
        "name": "pa-27n",
    },
    {
        "code": 32029,
        "datum": "nad27",
        "name": "pa-27s",
    },
    {
        "code": 32030,
        "datum": "nad27",
        "name": "ri-27",
    },
    {
        "code": 32031,
        "datum": "nad27",
        "name": "sc-27n",
    },
    {
        "code": 32033,
        "datum": "nad27",
        "name": "sc-27s",
    },
    {
        "code": 32034,
        "datum": "nad27",
        "name": "sd-27n",
    },
    {
        "code": 32035,
        "datum": "nad27",
        "name": "sd-27s",
    },
    {
        "code": 32036,
        "datum": "nad27",
        "name": "tn-27",
    },
    {
        "code": 32037,
        "datum": "nad27",
        "name": "tx-27n",
    },
    {
        "code": 32038,
        "datum": "nad27",
        "name": "tx-27nc",
    },
    {
        "code": 32039,
        "datum": "nad27",
        "name": "tx-27c",
    },
    {
        "code": 32040,
        "datum": "nad27",
        "name": "tx-27sc",
    },
    {
        "code": 32041,
        "datum": "nad27",
        "name": "tx-27s",
    },
    {
        "code": 32042,
        "datum": "nad27",
        "name": "ut-27n",
    },
    {
        "code": 32043,
        "datum": "nad27",
        "name": "ut-27c",
    },
    {
        "code": 32044,
        "datum": "nad27",
        "name": "ut-27s",
    },
    {
        "code": 32045,
        "datum": "nad27",
        "name": "vt-27",
    },
    {
        "code": 32046,
        "datum": "nad27",
        "name": "va-27n",
    },
    {
        "code": 32047,
        "datum": "nad27",
        "name": "va-27s",
    },
    {
        "code": 32048,
        "datum": "nad27",
        "name": "wa-27n",
    },
    {
        "code": 32049,
        "datum": "nad27",
        "name": "wa-27s",
    },
    {
        "code": 32050,
        "datum": "nad27",
        "name": "wv-27n",
    },
    {
        "code": 32051,
        "datum": "nad27",
        "name": "wv-27s",
    },
    {
        "code": 32052,
        "datum": "nad27",
        "name": "wi-27n",
    },
    {
        "code": 32053,
        "datum": "nad27",
        "name": "wi-27c",
    },
    {
        "code": 32054,
        "datum": "nad27",
        "name": "wi-27s",
    },
    {
        "code": 32055,
        "datum": "nad27",
        "name": "wy-27e",
    },
    {
        "code": 32056,
        "datum": "nad27",
        "name": "wy-27ec",
    },
    {
        "code": 32057,
        "datum": "nad27",
        "name": "wy-27wc",
    },
    {
        "code": 32058,
        "datum": "nad27",
        "name": "wy-27w",
    },
    {
        "code": 32059,
        "datum": "nad27",
        "name": "prvi-27",
    },
    {
        "code": 32060,
        "datum": "nad27",
        "name": "stcr-27",
    },
    {
        "code": 26929,
        "datum": "nad83",
        "name": "al83-e",
    },
    {
        "code": 26930,
        "datum": "nad83",
        "name": "al83-w",
    },
    {
        "code": 26931,
        "datum": "nad83",
        "name": "ak83-1",
    },
    {
        "code": 26932,
        "datum": "nad83",
        "name": "ak83-2",
    },
    {
        "code": 26933,
        "datum": "nad83",
        "name": "ak83-3",
    },
    {
        "code": 26934,
        "datum": "nad83",
        "name": "ak83-4",
    },
    {
        "code": 26935,
        "datum": "nad83",
        "name": "ak83-5",
    },
    {
        "code": 26936,
        "datum": "nad83",
        "name": "ak83-6",
    },
    {
        "code": 26937,
        "datum": "nad83",
        "name": "ak83-7",
    },
    {
        "code": 26938,
        "datum": "nad83",
        "name": "ak83-8",
    },
    {
        "code": 26939,
        "datum": "nad83",
        "name": "ak83-9",
    },
    {
        "code": 26940,
        "datum": "nad83",
        "name": "ak83-10",
    },
    {
        "code": 26948,
        "datum": "nad83",
        "name": "az83-e",
    },
    {
        "code": 26949,
        "datum": "nad83",
        "name": "az83-c",
    },
    {
        "code": 26950,
        "datum": "nad83",
        "name": "az83-w",
    },
    {
        "code": 26951,
        "datum": "nad83",
        "name": "ar83-n",
    },
    {
        "code": 26952,
        "datum": "nad83",
        "name": "ar83-s",
    },
    {
        "code": 26941,
        "datum": "nad83",
        "name": "ca83-1",
    },
    {
        "code": 26942,
        "datum": "nad83",
        "name": "ca83-2",
    },
    {
        "code": 26943,
        "datum": "nad83",
        "name": "ca83-3",
    },
    {
        "code": 26944,
        "datum": "nad83",
        "name": "ca83-4",
    },
    {
        "code": 26945,
        "datum": "nad83",
        "name": "ca83-5",
    },
    {
        "code": 26946,
        "datum": "nad83",
        "name": "ca83-6",
    },
    {
        "code": 26953,
        "datum": "nad83",
        "name": "co83-n",
    },
    {
        "code": 26954,
        "datum": "nad83",
        "name": "co83-c",
    },
    {
        "code": 26955,
        "datum": "nad83",
        "name": "co83-s",
    },
    {
        "code": 26956,
        "datum": "nad83",
        "name": "ct83",
    },
    {
        "code": 26957,
        "datum": "nad83",
        "name": "de83",
    },
    {
        "code": 26960,
        "datum": "nad83",
        "name": "fl83-n",
    },
    {
        "code": 26958,
        "datum": "nad83",
        "name": "fl83-e",
    },
    {
        "code": 26959,
        "datum": "nad83",
        "name": "fl83-w",
    },
    {
        "code": 26966,
        "datum": "nad83",
        "name": "ga83-e",
    },
    {
        "code": 26967,
        "datum": "nad83",
        "name": "ga83-w",
    },
    {
        "code": 26961,
        "datum": "nad83",
        "name": "hi83-1",
    },
    {
        "code": 26962,
        "datum": "nad83",
        "name": "hi83-2",
    },
    {
        "code": 26963,
        "datum": "nad83",
        "name": "hi83-3",
    },
    {
        "code": 26964,
        "datum": "nad83",
        "name": "hi83-4",
    },
    {
        "code": 26965,
        "datum": "nad83",
        "name": "hi83-5",
    },
    {
        "code": 26968,
        "datum": "nad83",
        "name": "id83-e",
    },
    {
        "code": 26969,
        "datum": "nad83",
        "name": "id83-c",
    },
    {
        "code": 26970,
        "datum": "nad83",
        "name": "id83-w",
    },
    {
        "code": 26971,
        "datum": "nad83",
        "name": "il83-e",
    },
    {
        "code": 26972,
        "datum": "nad83",
        "name": "il83-w",
    },
    {
        "code": 26973,
        "datum": "nad83",
        "name": "ia83-e",
    },
    {
        "code": 26974,
        "datum": "nad83",
        "name": "ia83-w",
    },
    {
        "code": 26975,
        "datum": "nad83",
        "name": "io83-n",
    },
    {
        "code": 26976,
        "datum": "nad83",
        "name": "io83-s",
    },
    {
        "code": 26977,
        "datum": "nad83",
        "name": "ks83-n",
    },
    {
        "code": 26978,
        "datum": "nad83",
        "name": "ks83-s",
    },
    {
        "code": 26979,
        "datum": "nad83",
        "name": "ky83-n",
    },
    {
        "code": 26980,
        "datum": "nad83",
        "name": "ky83-s",
    },
    {
        "code": 26981,
        "datum": "nad83",
        "name": "la83-n",
    },
    {
        "code": 26982,
        "datum": "nad83",
        "name": "la83-s",
    },
    {
        "code": 26983,
        "datum": "nad83",
        "name": "me83-e",
    },
    {
        "code": 26984,
        "datum": "nad83",
        "name": "me83-w",
    },
    {
        "code": 26985,
        "datum": "nad83",
        "name": "md83",
    },
    {
        "code": 26986,
        "datum": "nad83",
        "name": "ma83-m",
    },
    {
        "code": 26987,
        "datum": "nad83",
        "name": "ma83-i",
    },
    {
        "code": 26988,
        "datum": "nad83",
        "name": "mi83-n",
    },
    {
        "code": 26989,
        "datum": "nad83",
        "name": "mi83-c",
    },
    {
        "code": 26990,
        "datum": "nad83",
        "name": "mi83-s",
    },
    {
        "code": 26991,
        "datum": "nad83",
        "name": "mn83-n",
    },
    {
        "code": 26992,
        "datum": "nad83",
        "name": "mn83-c",
    },
    {
        "code": 26993,
        "datum": "nad83",
        "name": "mn83-s",
    },
    {
        "code": 26994,
        "datum": "nad83",
        "name": "ms83-e",
    },
    {
        "code": 26995,
        "datum": "nad83",
        "name": "ms83-w",
    },
    {
        "code": 26996,
        "datum": "nad83",
        "name": "mo83-e",
    },
    {
        "code": 26997,
        "datum": "nad83",
        "name": "mo83-c",
    },
    {
        "code": 26998,
        "datum": "nad83",
        "name": "mo83-w",
    },
    {
        "code": 32100,
        "datum": "nad83",
        "name": "mt83",
    },
    {
        "code": 32104,
        "datum": "nad83",
        "name": "ne83",
    },
    {
        "code": 32107,
        "datum": "nad83",
        "name": "nv83-e",
    },
    {
        "code": 32108,
        "datum": "nad83",
        "name": "nv83-c",
    },
    {
        "code": 32109,
        "datum": "nad83",
        "name": "nv83-w",
    },
    {
        "code": 32110,
        "datum": "nad83",
        "name": "nh83",
    },
    {
        "code": 32111,
        "datum": "nad83",
        "name": "nj83",
    },
    {
        "code": 32112,
        "datum": "nad83",
        "name": "nm83-e",
    },
    {
        "code": 32113,
        "datum": "nad83",
        "name": "nm83-c",
    },
    {
        "code": 32114,
        "datum": "nad83",
        "name": "nm83-w",
    },
    {
        "code": 32115,
        "datum": "nad83",
        "name": "ny83-e",
    },
    {
        "code": 32116,
        "datum": "nad83",
        "name": "ny83-c",
    },
    {
        "code": 32117,
        "datum": "nad83",
        "name": "ny83-w",
    },
    {
        "code": 32118,
        "datum": "nad83",
        "name": "ny83-i",
    },
    {
        "code": 32119,
        "datum": "nad83",
        "name": "nc83",
    },
    {
        "code": 32120,
        "datum": "nad83",
        "name": "nd83-n",
    },
    {
        "code": 32121,
        "datum": "nad83",
        "name": "nd83-s",
    },
    {
        "code": 32122,
        "datum": "nad83",
        "name": "oh83-n",
    },
    {
        "code": 32123,
        "datum": "nad83",
        "name": "oh83-s",
    },
    {
        "code": 32124,
        "datum": "nad83",
        "name": "ok83-n",
    },
    {
        "code": 32125,
        "datum": "nad83",
        "name": "ok83-s",
    },
    {
        "code": 32126,
        "datum": "nad83",
        "name": "or83-n",
    },
    {
        "code": 32127,
        "datum": "nad83",
        "name": "or83-s",
    },
    {
        "code": 32128,
        "datum": "nad83",
        "name": "pa83-n",
    },
    {
        "code": 32129,
        "datum": "nad83",
        "name": "pa83-s",
    },
    {
        "code": 32161,
        "datum": "nad83",
        "name": "prvi83",
    },
    {
        "code": 32130,
        "datum": "nad83",
        "name": "ri83",
    },
    {
        "code": 32133,
        "datum": "nad83",
        "name": "sc83",
    },
    {
        "code": 32134,
        "datum": "nad83",
        "name": "sd83-n",
    },
    {
        "code": 32135,
        "datum": "nad83",
        "name": "sd83-s",
    },
    {
        "code": 32136,
        "datum": "nad83",
        "name": "tn83",
    },
    {
        "code": 32137,
        "datum": "nad83",
        "name": "tx83-n",
    },
    {
        "code": 32138,
        "datum": "nad83",
        "name": "tx83-nc",
    },
    {
        "code": 32139,
        "datum": "nad83",
        "name": "tx83-c",
    },
    {
        "code": 32140,
        "datum": "nad83",
        "name": "tx83-sc",
    },
    {
        "code": 32141,
        "datum": "nad83",
        "name": "tx83-s",
    },
    {
        "code": 32142,
        "datum": "nad83",
        "name": "ut83-n",
    },
    {
        "code": 32143,
        "datum": "nad83",
        "name": "ut83-c",
    },
    {
        "code": 32144,
        "datum": "nad83",
        "name": "ut83-s",
    },
    {
        "code": 32145,
        "datum": "nad83",
        "name": "vt83",
    },
    {
        "code": 32146,
        "datum": "nad83",
        "name": "va83-n",
    },
    {
        "code": 32147,
        "datum": "nad83",
        "name": "va83-s",
    },
    {
        "code": 32148,
        "datum": "nad83",
        "name": "wa83-n",
    },
    {
        "code": 32149,
        "datum": "nad83",
        "name": "wa83-s",
    },
    {
        "code": 32150,
        "datum": "nad83",
        "name": "wv83-n",
    },
    {
        "code": 32151,
        "datum": "nad83",
        "name": "wv83-s",
    },
    {
        "code": 32152,
        "datum": "nad83",
        "name": "wi83-n",
    },
    {
        "code": 32153,
        "datum": "nad83",
        "name": "wi83-c",
    },
    {
        "code": 32155,
        "datum": "nad83",
        "name": "wy83-e",
    },
    {
        "code": 32156,
        "datum": "nad83",
        "name": "wy83-ec",
    },
    {
        "code": 32157,
        "datum": "nad83",
        "name": "wy83-wc",
    },
    {
        "code": 32182,
        "datum": "nad83",
        "name": "qmtm-2",
    },
    {
        "code": 32183,
        "datum": "nad83",
        "name": "qmtm-3",
    },
    {
        "code": 32184,
        "datum": "nad83",
        "name": "qmtm-4",
    },
    {
        "code": 32185,
        "datum": "nad83",
        "name": "qmtm-5",
    },
    {
        "code": 32186,
        "datum": "nad83",
        "name": "qmtm-6",
    },
    {
        "code": 32187,
        "datum": "nad83",
        "name": "qmtm-7",
    },
    {
        "code": 32188,
        "datum": "nad83",
        "name": "qmtm-8",
    },
    {
        "code": 32189,
        "datum": "nad83",
        "name": "qmtm-9",
    },
    {
        "code": 32190,
        "datum": "nad83",
        "name": "qmtm-10",
    },
    {
        "code": 27700,
        "datum": "ord surv gb",
        "name": "national-grid",
    },
    {
        "code": 21500,
        "datum": "belgium",
        "name": "bns50",
    },
    {
        "code": 30161,
        "datum": "tokyo-japan",
        "name": "zone i",
    },
    {
        "code": 30162,
        "datum": "tokyo-japan",
        "name": "zone ii",
    },
    {
        "code": 30163,
        "datum": "tokyo-japan",
        "name": "zone iii",
    },
    {
        "code": 30164,
        "datum": "tokyo-japan",
        "name": "zone iv",
    },
    {
        "code": 30165,
        "datum": "tokyo-japan",
        "name": "zone v",
    },
    {
        "code": 30166,
        "datum": "tokyo-japan",
        "name": "zone vi",
    },
    {
        "code": 30167,
        "datum": "tokyo-japan",
        "name": "zone vii",
    },
    {
        "code": 30168,
        "datum": "tokyo-japan",
        "name": "zone viii",
    },
    {
        "code": 30169,
        "datum": "tokyo-japan",
        "name": "zone ix",
    },
    {
        "code": 30170,
        "datum": "tokyo-japan",
        "name": "zone x",
    },
    {
        "code": 30171,
        "datum": "tokyo-japan",
        "name": "zone xi",
    },
    {
        "code": 30172,
        "datum": "tokyo-japan",
        "name": "zone xii",
    },
    {
        "code": 30173,
        "datum": "tokyo-japan",
        "name": "zone xiii",
    },
    {
        "code": 30174,
        "datum": "tokyo-japan",
        "name": "zone xiv",
    },
    {
        "code": 30175,
        "datum": "tokyo-japan",
        "name": "zone xv",
    },
    {
        "code": 30176,
        "datum": "tokyo-japan",
        "name": "zone xvi",
    },
    {
        "code": 30177,
        "datum": "tokyo-japan",
        "name": "zone xvii",
    },
    {
        "code": 30178,
        "datum": "tokyo-japan",
        "name": "zone xviii",
    },
    {
        "code": 30179,
        "datum": "tokyo-japan",
        "name": "zone xix",
    },
    {
        "code": 28402,
        "datum": "pulkovo 1942",
        "name": "gk-02",
    },
    {
        "code": 28403,
        "datum": "pulkovo 1942",
        "name": "gk-03",
    },
    {
        "code": 28404,
        "datum": "pulkovo 1942",
        "name": "gk-04",
    },
    {
        "code": 28405,
        "datum": "pulkovo 1942",
        "name": "gk-05",
    },
    {
        "code": 28406,
        "datum": "pulkovo 1942",
        "name": "gk-06",
    },
    {
        "code": 28407,
        "datum": "pulkovo 1942",
        "name": "gk-07",
    },
    {
        "code": 28408,
        "datum": "pulkovo 1942",
        "name": "gk-08",
    },
    {
        "code": 28409,
        "datum": "pulkovo 1942",
        "name": "gk-09",
    },
    {
        "code": 28410,
        "datum": "pulkovo 1942",
        "name": "gk-10",
    },
    {
        "code": 28411,
        "datum": "pulkovo 1942",
        "name": "gk-11",
    },
    {
        "code": 28412,
        "datum": "pulkovo 1942",
        "name": "gk-12",
    },
    {
        "code": 28413,
        "datum": "pulkovo 1942",
        "name": "gk-13",
    },
    {
        "code": 28414,
        "datum": "pulkovo 1942",
        "name": "gk-14",
    },
    {
        "code": 28415,
        "datum": "pulkovo 1942",
        "name": "gk-15",
    },
    {
        "code": 28416,
        "datum": "pulkovo 1942",
        "name": "gk-16",
    },
    {
        "code": 28417,
        "datum": "pulkovo 1942",
        "name": "gk-17",
    },
    {
        "code": 28418,
        "datum": "pulkovo 1942",
        "name": "gk-18",
    },
    {
        "code": 28419,
        "datum": "pulkovo 1942",
        "name": "gk-19",
    },
    {
        "code": 28420,
        "datum": "pulkovo 1942",
        "name": "gk-20",
    },
    {
        "code": 28421,
        "datum": "pulkovo 1942",
        "name": "gk-21",
    },
    {
        "code": 28422,
        "datum": "pulkovo 1942",
        "name": "gk-22",
    },
    {
        "code": 28423,
        "datum": "pulkovo 1942",
        "name": "gk-23",
    },
    {
        "code": 28424,
        "datum": "pulkovo 1942",
        "name": "gk-24",
    },
    {
        "code": 28425,
        "datum": "pulkovo 1942",
        "name": "gk-25",
    },
    {
        "code": 28426,
        "datum": "pulkovo 1942",
        "name": "gk-26",
    },
    {
        "code": 28427,
        "datum": "pulkovo 1942",
        "name": "gk-27",
    },
    {
        "code": 28428,
        "datum": "pulkovo 1942",
        "name": "gk-28",
    },
    {
        "code": 28429,
        "datum": "pulkovo 1942",
        "name": "gk-29",
    },
    {
        "code": 28430,
        "datum": "pulkovo 1942",
        "name": "gk-30",
    },
    {
        "code": 28431,
        "datum": "pulkovo 1942",
        "name": "gk-31",
    },
    {
        "code": 28432,
        "datum": "pulkovo 1942",
        "name": "gk-32",
    },
    {
        "code": 28462,
        "datum": "pulkovo 1942",
        "name": "gk-02n",
    },
    {
        "code": 28463,
        "datum": "pulkovo 1942",
        "name": "gk-03n",
    },
    {
        "code": 28464,
        "datum": "pulkovo 1942",
        "name": "gk-04n",
    },
    {
        "code": 28465,
        "datum": "pulkovo 1942",
        "name": "gk-05n",
    },
    {
        "code": 28466,
        "datum": "pulkovo 1942",
        "name": "gk-06n",
    },
    {
        "code": 28467,
        "datum": "pulkovo 1942",
        "name": "gk-07n",
    },
    {
        "code": 28468,
        "datum": "pulkovo 1942",
        "name": "gk-08n",
    },
    {
        "code": 28469,
        "datum": "pulkovo 1942",
        "name": "gk-09n",
    },
    {
        "code": 28470,
        "datum": "pulkovo 1942",
        "name": "gk-10n",
    },
    {
        "code": 28471,
        "datum": "pulkovo 1942",
        "name": "gk-11n",
    },
    {
        "code": 28472,
        "datum": "pulkovo 1942",
        "name": "gk-12n",
    },
    {
        "code": 28473,
        "datum": "pulkovo 1942",
        "name": "gk-13n",
    },
    {
        "code": 28474,
        "datum": "pulkovo 1942",
        "name": "gk-14n",
    },
    {
        "code": 28475,
        "datum": "pulkovo 1942",
        "name": "gk-15n",
    },
    {
        "code": 28476,
        "datum": "pulkovo 1942",
        "name": "gk-16n",
    },
    {
        "code": 28477,
        "datum": "pulkovo 1942",
        "name": "gk-17n",
    },
    {
        "code": 28478,
        "datum": "pulkovo 1942",
        "name": "gk-18n",
    },
    {
        "code": 28479,
        "datum": "pulkovo 1942",
        "name": "gk-19n",
    },
    {
        "code": 28480,
        "datum": "pulkovo 1942",
        "name": "gk-20n",
    },
    {
        "code": 28481,
        "datum": "pulkovo 1942",
        "name": "gk-21n",
    },
    {
        "code": 28482,
        "datum": "pulkovo 1942",
        "name": "gk-22n",
    },
    {
        "code": 28483,
        "datum": "pulkovo 1942",
        "name": "gk-23n",
    },
    {
        "code": 28484,
        "datum": "pulkovo 1942",
        "name": "gk-24n",
    },
    {
        "code": 28485,
        "datum": "pulkovo 1942",
        "name": "gk-25n",
    },
    {
        "code": 28486,
        "datum": "pulkovo 1942",
        "name": "gk-26n",
    },
    {
        "code": 28487,
        "datum": "pulkovo 1942",
        "name": "gk-27n",
    },
    {
        "code": 28488,
        "datum": "pulkovo 1942",
        "name": "gk-28n",
    },
    {
        "code": 28489,
        "datum": "pulkovo 1942",
        "name": "gk-29n",
    },
    {
        "code": 28490,
        "datum": "pulkovo 1942",
        "name": "gk-30n",
    },
    {
        "code": 28491,
        "datum": "pulkovo 1942",
        "name": "gk-31n",
    },
    {
        "code": 28492,
        "datum": "pulkovo 1942",
        "name": "gk-32n",
    },
    {
        "code": 31461,
        "datum": "dhdn",
        "name": "gk3-1",
    },
    {
        "code": 31462,
        "datum": "dhdn",
        "name": "gk3-2",
    },
    {
        "code": 31463,
        "datum": "dhdn",
        "name": "gk3-3",
    },
    {
        "code": 31464,
        "datum": "dhdn",
        "name": "gk3-4",
    },
    {
        "code": 31465,
        "datum": "dhdn",
        "name": "gk3-5",
    },
    {
        "code": 27291,
        "datum": "geodetic49",
        "name": "nznygn",
    },
    {
        "code": 27292,
        "datum": "geodetic49",
        "name": "nznygs",
    },
    {
        "code": 28992,
        "datum": "netherlands",
        "name": "netherlands national system",
    },
    {
        "code": 28600,
        "datum": "qatar national 1995",
        "name": "qatar national grid",
    },
    {
        "code": 22191,
        "datum": "campo inch",
        "name": "argentina zone i",
    },
    {
        "code": 22192,
        "datum": "campo inch",
        "name": "argentina zone ii",
    },
    {
        "code": 22193,
        "datum": "campo inch",
        "name": "argentina zone iii",
    },
    {
        "code": 22194,
        "datum": "campo inch",
        "name": "argentina zone iv",
    },
    {
        "code": 22195,
        "datum": "campo inch",
        "name": "argentina zone v",
    },
    {
        "code": 22196,
        "datum": "campo inch",
        "name": "argentina zone vi",
    },
    {
        "code": 22197,
        "datum": "campo inch",
        "name": "argentina zone vii",
    },
    {
        "code": 22922,
        "datum": "old egyptian",
        "name": "egypt red belt",
    },
    {
        "code": 22993,
        "datum": "old egyptian",
        "name": "egypt purple belt",
    },
    {
        "code": 22994,
        "datum": "old egyptian",
        "name": "extended purple belt",
    },
    {
        "code": 24370,
        "datum": "kalianpur",
        "name": "kalianpur zone-0",
    },
    {
        "code": 24371,
        "datum": "kalianpur",
        "name": "kalianpur zone-1",
    },
    {
        "code": 24372,
        "datum": "kalianpur",
        "name": "kalianpur zone-2a",
    },
    {
        "code": 24382,
        "datum": "kalianpur",
        "name": "kalianpur zone-2b",
    },
    {
        "code": 24373,
        "datum": "kalianpur",
        "name": "kalianpur zone-3a",
    },
    {
        "code": 24383,
        "datum": "kalianpur",
        "name": "kalianpur zone-3b",
    },
    {
        "code": 24374,
        "datum": "kalianpur",
        "name": "kalianpur zone-4a",
    },
    {
        "code": 24384,
        "datum": "kalianpur",
        "name": "kalianpur zone-4b",
    },
    {
        "code": 26391,
        "datum": "minna-nigeria",
        "name": "minna nigeria west belt",
    },
    {
        "code": 26392,
        "datum": "minna-nigeria",
        "name": "minna nigeria mid belt",
    },
    {
        "code": 26393,
        "datum": "minna-nigeria",
        "name": "minna nigeria east belt",
    },
    {
        "code": 25391,
        "datum": "luzon",
        "name": "luzon zone-i",
    },
    {
        "code": 25392,
        "datum": "luzon",
        "name": "luzon zone-ii",
    },
    {
        "code": 25393,
        "datum": "luzon",
        "name": "luzon zone-iii",
    },
    {
        "code": 25394,
        "datum": "luzon",
        "name": "luzon zone-iv",
    },
    {
        "code": 25395,
        "datum": "luzon",
        "name": "luzon zone-v",
    },
]


# def lookup_epsg(
#     datum: Optional[Union[int, str]], projection: Optional[Union[int, str]]
# ) -> Dict[str, Union[int, str]]:
#     storage_epsg: int = 0
#     storage_name: str = "unknown"
#     display_epsg: int = 0
#     display_name: str = "unknown"

#     if datum and re.match(r"^\d+$", str(datum)):
#         storage_epsg = int(datum)
#     else:
#         o = next((x for x in geodetics if x["name"] == str(datum).lower()), None)
#         storage_epsg = o.get("code", 0) if o else 0
#         storage_name = o.get("name", "unknown") if o else "unknown"

#     if projection and re.match(r"^\d+$", str(projection)):
#         display_epsg = int(projection)
#     else:
#         o: Optional[Dict[str, Union[int, str]]] = next(
#             (x for x in projections if x["name"] == str(projection).lower()), None
#         )
#         display_epsg = o.get("code", 0) if o else 0
#         display_name = o.get("name", "unknown") if o else "unknown"

#     result = {
#         "storage_epsg": storage_epsg,
#         "storage_name": storage_name,
#         "display_epsg": display_epsg,
#         "display_name": display_name,
#     }
#     return result


def lookup_epsg(
    datum: Optional[Union[int, str]], projection: Optional[Union[int, str]]
) -> Dict[str, Union[int, str]]:
    """Guess storage and display n

    Args:
        datum (Optional[Union[int, str]]): "datum" (?) parsed from parms
        projection (Optional[Union[int, str]]): "projection" (?) from parms

    Returns:
        Dict[str, Union[int, str]]: A Dict of storage and display codes
    """
    storage_epsg: int = 0
    storage_name: str = "unknown"
    display_epsg: int = 0
    display_name: str = "unknown"

    if datum and re.match(r"^\d+$", str(datum)):
        storage_epsg = int(datum)
    else:
        o: Optional[Dict[str, Any]] = next(
            (x for x in geodetics if x["name"] == str(datum).lower()), None
        )
        storage_epsg = int(o.get("code", 0)) if o else 0
        storage_name = str(o.get("name", "unknown")) if o else "unknown"

    if projection and re.match(r"^\d+$", str(projection)):
        display_epsg = int(projection)
    else:
        o: Optional[Dict[str, Any]] = next(
            (x for x in projections if x["name"] == str(projection).lower()), None
        )
        display_epsg = int(o.get("code", 0)) if o else 0
        display_name = str(o.get("name", "unknown")) if o else "unknown"

    result: Dict[str, Union[int, str]] = {
        "storage_epsg": storage_epsg,
        "storage_name": storage_name,
        "display_epsg": display_epsg,
        "display_name": display_name,
    }
    return result


def epsg_codes(repo_base: Dict[str, Any]) -> Dict[str, Union[int, str]] | None:
    """Make an educated guess on likely storage and display EPSG codes.

    This is mostly based on epsg.io and may vary from the older BlueMarble stuff
    used by GeoPlus. Any real GIS work should verify things first.

    Args:
        repo_base (Dict[str, Any]): A stub repo dict.

    Returns:
        Dict[str, Union[int, str]] | None: EPSG names and codes
    """
    conn: Dict[str, str] = {
        "driver": repo_base["conn"]["driver"],
        "catalogname": repo_base["fs_path"] + "/PARMS",
    }

    sql = "SELECT ObjValue FROM pubparms WHERE parmid = 40"
    data: List[Dict[str, Any]] | Exception = db_exec(conn, sql)
    if isinstance(data, Exception):
        logger.error(data)
        return None

    blob: Any = data[0]["ObjValue"]
    buf = bytearray(blob)

    prj_val = buf[2537:2601]
    projection = prj_val.decode("utf-8").split("\x00")[0]

    dtm_val = buf[2602:]
    datum = dtm_val.decode("utf-8").split("\x00")[0]

    result: Dict[str, Union[int, str]] = lookup_epsg(datum, projection)
    return result
