"""Petra survey"""

from purr_petra.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE

identifier_keys = ["w.wsn"]
id_form = " || '-' || ".join([f"CAST({i} AS VARCHAR(10))" for i in identifier_keys])

selector = f"""
    SELECT
        w.wsn          AS w_wsn,
        w.uwi          AS w_uwi,

        s.lat          AS s_lat,
        s.lon          AS s_lon,

        u.wsn          AS u_wsn,
        u.uwi          AS u_uwi,
        u.label        AS u_label,
        u.sortname     AS u_sortname,
        u.flags        AS u_flags,

        d.survrecid    AS d_survrecid,
        d.wsn          AS d_wsn,
        d.flags        AS d_flags,
        d.datasize     AS d_datasize,
        d.active       AS d_active,
        d.adddate      AS d_adddate,
        d.chgdate      AS d_chgdate,
        d.numrecs      AS d_numrecs,
        d.md1          AS d_md1,
        d.md2          AS d_md2,
        d.tvd1         AS d_tvd1,
        d.tvd2         AS d_tvd2,
        d.xoff1        AS d_xoff1,
        d.xoff2        AS d_xoff2,
        d.yoff1        AS d_yoff1,
        d.yoff2        AS d_yoff2,
        d.xyunits      AS d_xyunits,
        d.depunits     AS d_depunits,
        d.dippresent   AS d_dippresent,
        d.remarks      AS d_remarks,
        d.vs_1         AS d_vs_1,
        d.vs_2         AS d_vs_2,
        d.vs_3         AS d_vs_3,
        --d.data         AS d_data,

        f.name         AS f_survey_type,

        LIST(COALESCE(CAST(v.wsn AS VARCHAR(10)),      '{PURR_NULL}'), '{PURR_DELIM}') AS v_wsn,
        LIST(COALESCE(CAST(v.md/100 AS VARCHAR(10)),   '{PURR_NULL}'), '{PURR_DELIM}') AS v_md,
        LIST(COALESCE(CAST(v.tvd AS VARCHAR(20)),      '{PURR_NULL}'), '{PURR_DELIM}') AS v_tvd,
        LIST(COALESCE(CAST(v.xoff AS VARCHAR(20)),     '{PURR_NULL}'), '{PURR_DELIM}') AS v_xoff,
        LIST(COALESCE(CAST(v.yoff AS VARCHAR(20)),     '{PURR_NULL}'), '{PURR_DELIM}') AS v_yoff,
        LIST(COALESCE(CAST(v.dip AS VARCHAR(20)),      '{PURR_NULL}'), '{PURR_DELIM}') AS v_dip,
        LIST(COALESCE(CAST(v.azm AS VARCHAR(20)),      '{PURR_NULL}'), '{PURR_DELIM}') AS v_azm,
        LIST(COALESCE(CAST(v.vsection AS VARCHAR(20)), '{PURR_NULL}'), '{PURR_DELIM}') AS v_vsection,
        LIST(COALESCE(CAST(v.d1 AS VARCHAR(20)),       '{PURR_NULL}'), '{PURR_DELIM}') AS v_d1,
        LIST(COALESCE(CAST(v.d2 AS VARCHAR(20)),       '{PURR_NULL}'), '{PURR_DELIM}') AS v_d2,
        LIST(COALESCE(CAST(v.d3 AS VARCHAR(20)),       '{PURR_NULL}'), '{PURR_DELIM}') AS v_d3

    FROM well w
    LEFT OUTER JOIN dirsurv v ON v.wsn = w.wsn
    JOIN dirsurvdata d ON d.wsn = w.wsn
    JOIN dirsurvdef f ON f.survrecid = d.survrecid
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    {PURR_WHERE}
    GROUP BY w.wsn
    """

identifier = f"""
    SELECT
        {id_form} AS key
    FROM well w
    JOIN dirsurvdata d ON d.wsn = w.wsn
    JOIN dirsurvdef f ON f.survrecid = d.survrecid
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
        "d_": "dirsurvdata",
        "f_": "dirsurvdef",
        "v_": "dirsurv",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "d_adddate": "excel_date",
        "d_chgdate": "excel_date",
        # "d_data": "blob_to_hex",
        "v_wsn": "array_of_int",
        "v_md": "array_of_float",
        "v_tvd": "array_of_float",
        "v_xoff": "array_of_float",
        "v_yoff": "array_of_float",
        "v_dip": "array_of_float",
        "v_azm": "array_of_float",
        "v_vsection": "array_of_float",
        "v_d1": "array_of_float",
        "v_d2": "array_of_float",
        "v_d3": "array_of_float",
    },
    "chunk_size": 1000,
}
