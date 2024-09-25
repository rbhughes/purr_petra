"""Petra formation"""

from purr_petra.assets.collect.xformer import PURR_WHERE

identifier_keys = ["w.wsn", "f.fid"]
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

        f.fid          AS f_fid,
        f.zid          AS f_zid,
        f.name         AS f_name,
        f.source       AS f_source,
        f.desc         AS f_desc,
        f.units        AS f_units,
        f.kind         AS f_kind,
        f.ndec         AS f_ndec,
        f.adddate      AS f_adddate,
        f.chgdate      AS f_chgdate,
        f.remarks      AS f_remarks,
        f.flags        AS f_flags,
        f.unitstype    AS f_unitstype,

        z.fid          AS z_fid,
        z.wsn          AS z_wsn,
        z.zid          AS z_zid,
        z.z            AS z_z,
        z.postdepth    AS z_postdepth,
        z.quality      AS z_quality,
        z.symbol       AS z_symbol,
        z.chgdate      AS z_chgdate,
        z.textlen      AS z_textlen,
        z.text         AS z_text,
        z.datalen      AS z_datalen,
        z.data         AS z_data,

        t.recid        AS t_recid,
        t.wsn          AS t_wsn,
        t.fid          AS t_fid,
        t.flags        AS t_flags,
        t.symbol       AS t_symbol,
        t.iunits       AS t_iunits,
        t.npts         AS t_npts,
        t.datasize     AS t_datasize,
        t.adddate      AS t_adddate,
        t.chgdate      AS t_chgdate,
        t.data         AS t_data,
        CAST(t.remarks AS VARCHAR(512)) AS t_remarks

    FROM zflddef f
    JOIN zdata z ON f.fid = z.fid
    AND f.kind = 'T'
    AND z.zid = 1
    AND z.z < 1E30
    AND z.z IS NOT NULL
    LEFT OUTER JOIN zztops t ON t.wsn = z.wsn AND t.fid = z.fid
    JOIN well w ON z.wsn = w.wsn 
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    {PURR_WHERE}
    """

identifier = f"""
    SELECT
        LIST({id_form}) as keylist
    FROM zflddef f
    JOIN zdata z ON f.fid = z.fid
    AND f.kind = 'T'
    AND z.zid = 1
    AND z.z < 1E30
    AND z.z IS NOT NULL
    LEFT OUTER JOIN zztops t ON t.wsn = z.wsn AND t.fid = z.fid
    JOIN well w ON z.wsn = w.wsn 
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
        "f_": "zflddef",
        "z_": "zdata",
        "t_": "zztops",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "f_adddate": "excel_date",
        "f_chgdate": "excel_date",
        "f_remarks": "memo_to_string",
        "z_chgdate": "excel_date",
        "z_text": "memo_to_string",
        "z_data": "blob_to_hex",
        "t_adddate": "excel_date",
        "t_chgdate": "excel_date",
        "t_data": "parse_zztops",
    },
    "post_process": "formation_agg",
    "chunk_size": 1000,
}
