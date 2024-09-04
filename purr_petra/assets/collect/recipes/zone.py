"""Petra zone"""

from purr_petra.assets.collect.xformer import PURR_DELIM, PURR_NULL, PURR_WHERE

identifier_keys = ["w.wsn", "n.zid"]
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

        n.zid          AS n_zid,
        n.name         AS n_name,
        n.desc         AS n_desc,
        n.kind         AS n_kind,
        n.umode        AS n_umode,
        n.lmode        AS n_lmode,
        n.utopid       AS n_utopid,
        n.ltopid       AS n_ltopid,
        n.udepth       AS n_udepth,
        n.ldepth       AS n_ldepth,
        n.uoffset      AS n_uoffset,
        n.loffset      AS n_loffset,
        n.adddate      AS n_adddate,
        n.chgdate      AS n_chgdate,
        n.remarks      AS n_remarks,

        LIST(COALESCE(CAST(f.fid AS VARCHAR(10)),       '{PURR_NULL}'), '{PURR_DELIM}') AS f_fid,
        LIST(COALESCE(CAST(f.zid AS VARCHAR(10)),       '{PURR_NULL}'), '{PURR_DELIM}') AS f_zid,
        LIST(COALESCE(f.name,                           '{PURR_NULL}'), '{PURR_DELIM}') AS f_name,
        LIST(COALESCE(f.source,                         '{PURR_NULL}'), '{PURR_DELIM}') AS f_source,
        LIST(COALESCE(f.desc,                           '{PURR_NULL}'), '{PURR_DELIM}') AS f_desc,
        LIST(COALESCE(f.units,                          '{PURR_NULL}'), '{PURR_DELIM}') AS f_units,
        LIST(COALESCE(f.kind,                           '{PURR_NULL}'), '{PURR_DELIM}') AS f_kind,
        LIST(COALESCE(CAST(f.ndec AS VARCHAR(20)),      '{PURR_NULL}'), '{PURR_DELIM}') AS f_ndec,
        LIST(COALESCE(CAST(f.adddate AS VARCHAR(20)),   '{PURR_NULL}'), '{PURR_DELIM}') AS f_adddate,
        LIST(COALESCE(CAST(f.chgdate AS VARCHAR(20)),   '{PURR_NULL}'), '{PURR_DELIM}') AS f_chgdate,
        LIST(COALESCE(f.remarks,                        '{PURR_NULL}'), '{PURR_DELIM}') AS f_remarks,
        LIST(COALESCE(CAST(f.flags AS VARCHAR(20)),     '{PURR_NULL}'), '{PURR_DELIM}') AS f_flags,
        LIST(COALESCE(CAST(f.unitstype AS VARCHAR(20)), '{PURR_NULL}'), '{PURR_DELIM}') AS f_unitstype,
        
        LIST(COALESCE(CAST(z.fid AS VARCHAR(20)),       '{PURR_NULL}'), '{PURR_DELIM}') AS z_fid,
        LIST(COALESCE(CAST(z.wsn AS VARCHAR(20)),       '{PURR_NULL}'), '{PURR_DELIM}') AS z_wsn,
        LIST(COALESCE(CAST(z.zid AS VARCHAR(20)),       '{PURR_NULL}'), '{PURR_DELIM}') AS z_zid,
        LIST(COALESCE(CAST(z.z AS VARCHAR(20)),         '{PURR_NULL}'), '{PURR_DELIM}') AS z_z,
        LIST(COALESCE(CAST(z.postdepth AS VARCHAR(20)), '{PURR_NULL}'), '{PURR_DELIM}') AS z_postdepth,
        LIST(COALESCE(z.quality,                        '{PURR_NULL}'), '{PURR_DELIM}') AS z_quality,
        LIST(COALESCE(CAST(z.symbol AS VARCHAR(20)),    '{PURR_NULL}'), '{PURR_DELIM}') AS z_symbol,
        LIST(COALESCE(CAST(z.chgdate AS VARCHAR(20)),   '{PURR_NULL}'), '{PURR_DELIM}') AS z_chgdate,
        LIST(COALESCE(CAST(z.textlen AS VARCHAR(20)),   '{PURR_NULL}'), '{PURR_DELIM}') AS z_textlen,
        LIST(COALESCE(z.text,                           '{PURR_NULL}'), '{PURR_DELIM}') AS z_text,
        LIST(COALESCE(CAST(z.datalen AS VARCHAR(20)),   '{PURR_NULL}'), '{PURR_DELIM}') AS z_datalen,
        LIST(COALESCE(CAST(z.data AS VARCHAR(512)),     '{PURR_NULL}'), '{PURR_DELIM}') AS z_data

    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT JOIN locat s ON s.wsn = w.wsn
    JOIN zdata z ON z.wsn = w.wsn
    JOIN zonedef n ON n.zid = z.zid AND n.kind > 2
    JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid
    {PURR_WHERE}
    GROUP BY w.wsn, n.zid
    """

# identifier = f"""
#     SELECT
#         LIST({id_form}) AS key
#     FROM well w
#     LEFT JOIN uwi u ON u.wsn = w.wsn
#     JOIN zdata z ON z.wsn = w.wsn
#     JOIN zonedef n ON n.zid = z.zid AND n.kind > 2
#     JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid
#     {PURR_WHERE}
#     GROUP BY {id_form}
#     """

identifier = f"""
    SELECT DISTINCT
        {id_form} AS key
    FROM well w
    LEFT JOIN uwi u ON u.wsn = w.wsn
    JOIN zdata z ON z.wsn = w.wsn
    JOIN zonedef n ON n.zid = z.zid AND n.kind > 2
    JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid
    {PURR_WHERE}
    """

recipe = {
    "selector": selector,
    "identifier": identifier,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "u_": "uwi",
        "n_": "zonedef",
        "f_": "zflddef",
        "z_": "zdata",
    },
    "identifier_keys": identifier_keys,
    "xforms": {
        "w_chgdate": "excel_date",
        "f_fid": "array_of_int",
        "f_zid": "array_of_int",
        "f_name": "array_of_string",
        "f_source": "array_of_string",
        "f_desc": "array_of_string",
        "f_units": "array_of_string",
        "f_kind": "array_of_string",
        "f_ndec": "array_of_int",
        "f_adddate": "array_of_excel_date",
        "f_chgdate": "array_of_excel_date",
        "f_remarks": "array_of_string",
        "f_flags": "array_of_int",
        "f_unitstype": "array_of_int",
        "z_fid": "array_of_int",
        "z_wsn": "array_of_int",
        "z_zid": "array_of_int",
        "z_z": "array_of_float",
        "z_postdepth": "array_of_float",
        "z_quality": "array_of_string",
        "z_symbol": "array_of_int",
        "z_chgdate": "array_of_excel_date",
        "z_textlen": "array_of_int",
        "z_text": "array_of_string",
        "z_datalen": "array_of_int",
        "z_data": "array_of_string",  # array_of_hex?
        "n_adddate": "excel_date",
        "n_chgdate": "excel_date",
        "n_remarks": "memo_to_string",  # array_of_memo?
    },
    "post_process": "zone_agg",
    "chunk_size": 1000,
}
