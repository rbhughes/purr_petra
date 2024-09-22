"""Petra raster_log"""

from purr_petra.assets.collect.xformer import PURR_WHERE

identifier_keys = ["w.wsn", "i.ign"]
ID_FORM = " || '-' || ".join([f"CAST({i} AS VARCHAR(10))" for i in identifier_keys])

selector = f"""
    SELECT
        w.wsn           AS w_wsn,
        w.uwi           AS w_uwi,
        w.shortname     AS w_shortname,
        w.wellname      AS w_wellname,
        w.operator      AS w_operator,
        w.leasename     AS w_leasename,
        w.leasenumber   AS w_leasenumber,
        w.county        AS w_county,
        w.state         AS w_state,
        w.chgdate       AS w_chgdate,

        s.lat           AS s_lat,
        s.lon           AS s_lon,

        u.wsn           AS u_wsn,
        u.uwi           AS u_uwi,
        u.label         AS u_label,
        u.sortname      AS u_sortname,
        u.flags         AS u_flags,

        i.wsn           AS i_wsn,
        i.ign           AS i_ign,
        i.flags         AS i_flags,
        i.imagefilename AS i_imagefilename,
        i.calibfilename AS i_calibfilename,

        g.ign           AS g_ign,
        g.flags         AS g_flags,
        g.groupname     AS g_groupname,
        g.desc          AS g_desc,
        g.path          AS g_path

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN logimage i ON w.wsn = i.wsn
    LEFT OUTER JOIN logimgrp g ON i.ign = g.ign
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        LIST({ID_FORM}) AS keylist
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    JOIN logimage i ON w.wsn = i.wsn
    LEFT OUTER JOIN logimgrp g ON i.ign = g.ign
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "i_": "logimage",
        "g_": "logimgrp",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
    },
    "post_process": "raster_log_agg",
    "chunk_size": 5000,
}
