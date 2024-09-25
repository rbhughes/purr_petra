"""Petra core"""

from purr_petra.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE

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

        LIST(COALESCE(CAST(c.recid AS VARCHAR(10)),    '{PURR_NULL}'), '{PURR_DELIM}') AS c_recid,
        LIST(COALESCE(CAST(c.wsn AS VARCHAR(10)),      '{PURR_NULL}'), '{PURR_DELIM}') AS c_wsn,
        LIST(COALESCE(CAST(c.flags AS VARCHAR(10)),    '{PURR_NULL}'), '{PURR_DELIM}') AS c_flags,
        LIST(COALESCE(CAST(c.lithcode AS VARCHAR(10)), '{PURR_NULL}'), '{PURR_DELIM}') AS c_lithcode,
        LIST(COALESCE(CAST(c.date AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS c_date,
        LIST(COALESCE(CAST(c.top AS VARCHAR(10)),      '{PURR_NULL}'), '{PURR_DELIM}') AS c_top,
        LIST(COALESCE(CAST(c.base AS VARCHAR(10)),     '{PURR_NULL}'), '{PURR_DELIM}') AS c_base,
        LIST(COALESCE(CAST(c.recover AS VARCHAR(10)),  '{PURR_NULL}'), '{PURR_DELIM}') AS c_recover,
        LIST(COALESCE(c.type,                          '{PURR_NULL}'), '{PURR_DELIM}') AS c_type,
        LIST(COALESCE(c.qual,                          '{PURR_NULL}'), '{PURR_DELIM}') AS c_qual,
        LIST(COALESCE(c.fmname,                        '{PURR_NULL}'), '{PURR_DELIM}') AS c_fmname,
        LIST(COALESCE(c.desc,                          '{PURR_NULL}'), '{PURR_DELIM}') AS c_desc,
        LIST(COALESCE(CAST(c.remark AS VARCHAR(512)),  '{PURR_NULL}'), '{PURR_DELIM}') AS c_remark

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN cores c ON c.wsn = w.wsn
    {PURR_WHERE}
    GROUP BY w.wsn
    """


identifier = f"""
    SELECT
        DISTINCT({id_form}) AS key
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    JOIN cores c ON w.wsn = c.wsn
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "c_": "cores",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "c_recid": "array_of_int",
        "c_wsn": "array_of_int",
        "c_flags": "array_of_int",
        "c_lithcode": "array_of_int",
        "c_date": "array_of_excel_date",
        "c_top": "array_of_float",
        "c_base": "array_of_float",
        "c_recover": "array_of_float",
        "c_type": "array_of_string",
        "c_qual": "array_of_string",
        "c_fmname": "array_of_string",
        "c_desc": "array_of_string",
        "c_remark": "array_of_string",
    },
    "chunk_size": 1000,
}
