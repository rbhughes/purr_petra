"""Petra ip"""

from purr_petra.assets.collect.xformer import PURR_WHERE

identifier_keys = ["w.wsn", "p.recid"]
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

        p.recid        AS p_recid,
        p.wsn          AS p_wsn,
        p.numtreat     AS p_numtreat,
        p.flags        AS p_flags,
        p.date         AS p_date,
        p.top          AS p_top,
        p.base         AS p_base,
        p.oilvol       AS p_oilvol,
        p.gasvol       AS p_gasvol,
        p.wtrvol       AS p_wtrvol,
        p.ftp          AS p_ftp,
        p.fcp          AS p_fcp,
        p.stp          AS p_stp,
        p.scp          AS p_scp,
        p.bht          AS p_bht,
        p.bhp          AS p_bhp,
        p.choke        AS p_choke,
        p.duration     AS p_duration,
        p.caof         AS p_caof,
        p.oilgty       AS p_oilgty,
        p.gasgty       AS p_gasgty,
        p.gor          AS p_gor,
        p.testtype     AS p_testtype,
        p.fmname       AS p_fmname,
        p.oilunit      AS p_oilunit,
        p.gasunit      AS p_gasunit,
        p.wtrunit      AS p_wtrunit,
        p.remark       AS p_remark,
        p.treat        AS p_treat,
        p.chgdate      AS p_chgdate,
        p.unitstype    AS p_unitstype

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN pdtest p ON p.wsn = w.wsn
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        LIST({id_form}) AS keylist
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    JOIN pdtest p ON p.wsn = w.wsn
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "p_": "pdtest",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "p_date": "excel_date",
        "p_remark": "memo_to_string",
        "p_treat": "pdtest_treatment",
        "p_chgdate": "excel_date",
    },
    "post_process": "ip_agg",
    "chunk_size": 1000,
}
