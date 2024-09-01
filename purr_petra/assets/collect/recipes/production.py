"""Petra production"""

from purr_petra.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE

# identifier_keys = ["w.wsn", "f.mid"]
identifier_keys = ["w.wsn"]
id_form = " || '-' || ".join([f"CAST({i} AS VARCHAR(10))" for i in identifier_keys])

selector = f"""
    SELECT
        w.wsn          AS w_wsn,
        w.uwi          AS w_uwi,
        w.shortname    AS w_shortname,
        w.wellname     AS w_wellname,
        w.operator     AS w_operator,
        w.leasename    AS w_leasename,
        w.leasenumber  AS w_leasenumber,
        w.county       AS w_county,
        w.state        AS w_state,
        w.chgdate      AS w_chgdate,

        s.lat          AS s_lat,
        s.lon          AS s_lon,

        u.wsn          AS u_wsn,
        u.uwi          AS u_uwi,
        u.label        AS u_label,
        u.sortname     AS u_sortname,
        u.flags        AS u_flags,

        f.mid          AS f_mid,
        f.name         AS f_name,
        f.desc         AS f_desc,
        f.units        AS f_units,
        f.flags        AS f_flags,
        f.nullvalue    AS f_nullvalue,
        f.unitstype    AS f_unitstype,
        f.chgdate      AS f_chgdate,

        LIST(COALESCE(CAST(a.recid AS VARCHAR(10)),   '{PURR_NULL}'), '{PURR_DELIM}') AS a_recid,
        LIST(COALESCE(CAST(a.wsn AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_wsn,
        LIST(COALESCE(CAST(a.mid AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_mid,
        LIST(COALESCE(CAST(a.year AS VARCHAR(10)),    '{PURR_NULL}'), '{PURR_DELIM}') AS a_year,
        LIST(COALESCE(CAST(a.flags AS VARCHAR(10)),   '{PURR_NULL}'), '{PURR_DELIM}') AS a_flags,
        LIST(COALESCE(CAST(a.cum AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_cum,
        LIST(COALESCE(CAST(a.jan AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_jan,
        LIST(COALESCE(CAST(a.feb AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_feb,
        LIST(COALESCE(CAST(a.mar AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_mar,
        LIST(COALESCE(CAST(a.apr AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_apr,
        LIST(COALESCE(CAST(a.may AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_may,
        LIST(COALESCE(CAST(a.jun AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_jun,
        LIST(COALESCE(CAST(a.jul AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_jul,
        LIST(COALESCE(CAST(a.aug AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_aug,
        LIST(COALESCE(CAST(a.sep AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_sep,
        LIST(COALESCE(CAST(a.oct AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_oct,
        LIST(COALESCE(CAST(a.nov AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_nov,
        LIST(COALESCE(CAST(a.dec AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS a_dec,
        LIST(COALESCE(CAST(a.chgdate AS VARCHAR(10)), '{PURR_NULL}'), '{PURR_DELIM}') AS a_chgdate

    FROM mopddef f
    JOIN mopddata a ON a.mid = f.mid
    JOIN well w ON a.wsn = w.wsn
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    {PURR_WHERE}
    GROUP BY w.wsn, f.mid
    """

# identifier = f"""
#     SELECT
#         {id_form} AS key
#     FROM mopddef f
#     JOIN mopddata a ON a.mid = f.mid
#     JOIN well w ON a.wsn = w.wsn
#     LEFT JOIN uwi u ON u.wsn = w.wsn
#     {PURR_WHERE}
#     GROUP BY key
#     """

# identifier = f"""
#     SELECT DISTINCT
#         CAST(a.wsn AS VARCHAR(10)) || '-' || CAST(a.mid AS VARCHAR(10)) AS key
#     FROM mopddata a
#     JOIN well w ON a.wsn = w.wsn
#     LEFT JOIN uwi u ON u.wsn = w.wsn
#     {PURR_WHERE}
#     """

identifier = f"""
    SELECT DISTINCT
        {id_form} AS key
    FROM mopddata a
    JOIN well w ON a.wsn = w.wsn
    LEFT JOIN uwi u ON u.wsn = w.wsn
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "f_": "mopddef",
        "a_": "mopddata",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "f_chgdate": "excel_date",
        "a_recid": "array_of_int",
        "a_wsn": "array_of_int",
        "a_mid": "array_of_int",
        "a_year": "array_of_int",
        "a_flags": "array_of_int",
        "a_cum": "array_of_float",
        "a_jan": "array_of_float",
        "a_feb": "array_of_float",
        "a_mar": "array_of_float",
        "a_apr": "array_of_float",
        "a_may": "array_of_float",
        "a_jun": "array_of_float",
        "a_jul": "array_of_float",
        "a_aug": "array_of_float",
        "a_sep": "array_of_float",
        "a_oct": "array_of_float",
        "a_nov": "array_of_float",
        "a_dec": "array_of_float",
        "a_chgdate": "array_of_excel_date",
    },
    "post_process": "production_agg",
    "chunk_size": 1000,
}
