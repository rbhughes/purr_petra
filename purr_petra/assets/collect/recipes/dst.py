"""Petra dst"""

from purr_petra.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE

identifier_keys = ["w.wsn", "f.recid"]
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

        f.recid        AS f_recid,
        f.wsn          AS f_wsn,
        f.numrecov     AS f_numrecov,
        f.nummts       AS f_nummts,
        f.flags        AS f_flags,
        f.date         AS f_date,
        f.top          AS f_top,
        f.base         AS f_base,
        f.ihp          AS f_ihp,
        f.fhp          AS f_fhp,
        f.ffp          AS f_ffp,
        f.isp          AS f_isp,
        f.fsp          AS f_fsp,
        f.bht          AS f_bht,
        f.bhp          AS f_bhp,
        f.choke        AS f_choke,
        f.cushamt      AS f_cushamt,
        f.testtype     AS f_testtype,
        f.fmname       AS f_fmname,
        f.cushtype     AS f_cushtype,
        f.ohtime       AS f_ohtime,
        f.sitime       AS f_sitime,
        f.remark       AS f_remark,
        f.recov        AS f_recov,
        f.mts          AS f_mts,
        f.chgdate      AS f_chgdate,
        f.unitstype    AS f_unitstype

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        LIST({id_form}) AS keylist
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "f_": "fmtest",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "f_date": "excel_date",
        "f_remark": "memo_to_string",
        "f_recov": "fmtest_recovery",
        "f_mts": "memo_to_string",
        "f_chgdate": "excel_date",
    },
    "post_process": ["aggregate_fmtest"],
}
