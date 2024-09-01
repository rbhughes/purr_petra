"""Petra perforation

Petra's concept of perfs is less 'overwrought' than ggx. The sensible thing
is to consider common dates to signify a single completion event. The
completion may even be indicated by the API number's last 4 digits:
0001, 0002, 0003 signify recompletions
maybe in 11th position like 0100 if it's a horizontal well"
"""

from purr_petra.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE

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
        p.flags        AS p_flags,
        p.date         AS p_date,
        p.enddate      AS p_enddate,
        p.top          AS p_top,
        p.base         AS p_base,
        p.diameter     AS p_diameter,
        p.numshots     AS p_numshots,
        p.method       AS p_method,
        p.comptype     AS p_comptype,
        p.perftype     AS p_perftype,
        p.remark       AS p_remark,
        p.fmname       AS p_fmname,
        p.chgdate      AS p_chgdate,
        p.source       AS p_source

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN perfs p ON p.wsn = w.wsn
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        LIST({id_form}) AS keylist
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    JOIN perfs p ON p.wsn = w.wsn
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "p_": "perfs",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "p_date": "excel_date",
        "p_enddate": "excel_date",
        "p_remark": "memo_to_string",
        "p_chgdate": "excel_date",
    },
    "chunk_size": 5000,
}
