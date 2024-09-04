"""Petra vector_log"""

from purr_petra.assets.collect.xformer import PURR_WHERE

identifier_keys = ["w.wsn", "a.ldsn"]
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

    a.ldsn         AS a_ldsn,
    a.wsn          AS a_wsn,
    a.lsn          AS a_lsn,
    a.flags        AS a_flags,
    a.units        AS a_units,
    a.elev_zid     AS a_elev_zid,
    a.elev_fid     AS a_elev_fid,
    a.numpts       AS a_numpts,
    a.start        AS a_start,
    a.stop         AS a_stop,
    a.step         AS a_step,
    a.minval       AS a_minval,
    a.maxval       AS a_maxval,
    a.mean         AS a_mean,
    a.stddev       AS a_stddev,
    a.nullval      AS a_nullval,
    a.source       AS a_source,
    a.digits       AS a_digits,
    a.remarks      AS a_remarks,

    f.lsn          AS f_lsn,
    f.logname      AS f_logname,
    f.desc         AS f_desc,
    f.units        AS f_units,
    f.servid       AS f_servid,
    f.remarks      AS f_remarks,
    f.flags        AS f_flags,

    x.ldsn         AS x_ldsn,
    x.wsn          AS x_wsn,
    x.lsn          AS x_lsn,
    x.flags        AS x_flags,
    x.adddate      AS x_adddate,
    x.chgdate      AS x_chgdate,
    x.lasid        AS x_lasid,

    g.lasid        AS g_lasid,
    g.wsn          AS g_wsn,
    g.flags        AS g_flags,
    g.adddate      AS g_adddate,
    g.chgdate      AS g_chgdate,
    g.hdrsize      AS g_hdrsize,
    g.lashdr       AS g_lashdr

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN logdata a ON w.wsn = a.wsn
    JOIN logdef f ON a.lsn = f.lsn
    JOIN logdatax x ON a.wsn = x.wsn AND a.lsn = x.lsn AND a.ldsn = x.ldsn
    LEFT OUTER JOIN loglas g ON x.lasid = g.lasid AND w.wsn = g.wsn
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        LIST({id_form}) as keylist
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN logdata a ON w.wsn = a.wsn
    JOIN logdef f ON a.lsn = f.lsn
    JOIN logdatax x ON a.wsn = x.wsn AND a.lsn = x.lsn AND a.ldsn = x.ldsn
    LEFT OUTER JOIN loglas g ON x.lasid = g.lasid AND w.wsn = g.wsn
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "a_": "logdata",
        "f_": "logdef",
        "x_": "logdatax",
        "g_": "loglas",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "a_digits": "logdata_digits",
        "a_remarks": "memo_to_string",
        "x_adddate": "excel_date",
        "x_chgdate": "excel_date",
        "g_adddate": "excel_date",
        "g_chgdate": "excel_date",
        "g_lashdr": "loglas_lashdr",
    },
    "post_process": "vector_log_agg",
    "chunk_size": 1000,
}
