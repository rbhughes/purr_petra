"""Petra well"""

purr_where = "__pUrRwHeRe__"
identifier_keys = ["w.wsn"]
id_form = " || '-' || ".join([f"CAST({i} AS VARCHAR(10))" for i in identifier_keys])


recipe = {
    "selector": f"""
    SELECT
    w.wsn          AS w_wsn,
    w.flags        AS w_flags,
    w.adddate      AS w_adddate,
    w.chgdate      AS w_chgdate,
    w.elev_zid     AS w_elev_zid,
    w.elev_fid     AS w_elev_fid,
    w.symbol       AS w_symbol,
    w.uwi          AS w_uwi,
    w.label        AS w_label,
    w.shortname    AS w_shortname,
    w.wellname     AS w_wellname,
    w.symcode      AS w_symcode,
    w.operator     AS w_operator,
    w.histoper     AS w_histoper,
    w.leasename    AS w_leasename,
    w.leasenumber  AS w_leasenumber,
    w.fieldname    AS w_fieldname,
    w.fmattd       AS w_fmattd,
    w.prodfm       AS w_prodfm,
    w.county       AS w_county,
    w.state        AS w_state,
    w.remarks      AS w_remarks,
    
    s.wsn          AS s_wsn,
    s.flags        AS s_flags,
    s.x            AS s_x,
    s.y            AS s_y,
    s.z            AS s_z,
    s.lat          AS s_lat,
    s.lon          AS s_lon,
    s.botlat       AS s_botlat,
    s.botlon       AS s_botlon,
    s.botx         AS s_botx,
    s.boty         AS s_boty,
    s.congress     AS s_congress,
    s.congress     AS s_congress_orig,
    s.texasloc     AS s_texasloc,
    s.offshore     AS s_offshore,
    
    b.wsn          AS b_wsn,
    b.flags        AS b_flags,
    b.x            AS b_x,
    b.y            AS b_y,
    b.z            AS b_z,
    b.lat          AS b_lat,
    b.lon          AS b_lon,
    b.congress     AS b_congress,
    b.congress     AS b_congress_orig,
    b.texasloc     AS b_texasloc,
    b.offshore     AS b_offshore,
    b.chgdate      AS b_chgdate,
    
    z_1.z          AS z_elev_kb,
    z_2.z          AS z_elev_df,
    z_3.z          AS z_elev_gr,
    z_4.z          AS z_elev_seis,
    z_5.z          AS z_td,
    z_6.z          AS z_cumoil,
    z_7.z          AS z_cumgas,
    z_8.z          AS z_cumwtr,
    z_9.z          AS z_whipstock,
    z_10.z         AS z_wtrdepth,
    z_11.z         AS z_comp_date,
    z_12.z         AS z_spud_date,
    z_13.z         AS z_permit_date,
    z_14.z         AS z_rig_date,
    z_15.z         AS z_aband_date,
    z_16.z         AS z_report_date,
    z_17.z         AS z_wrs_date,
    z_18.z         AS z_last_act_date,
    z_19.text      AS z_platform,
    z_20.z         AS z_active_datum_value,
    
    u.wsn          AS u_wsn,
    u.uwi          AS u_uwi,
    u.label        AS u_label,
    u.sortname     AS u_sortname,
    u.flags        AS u_flags,      
    
    f.name         AS f_active_datum
    
    FROM well w
    LEFT JOIN locat s ON s.wsn = w.wsn
    LEFT JOIN bhloc b ON b.wsn = w.wsn
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT OUTER JOIN zdata z_1 ON w.wsn = z_1.wsn AND z_1.fid = 1
    LEFT OUTER JOIN zdata z_2 ON w.wsn = z_2.wsn AND z_2.fid = 2
    LEFT OUTER JOIN zdata z_3 ON w.wsn = z_3.wsn AND z_3.fid = 3
    LEFT OUTER JOIN zdata z_4 ON w.wsn = z_4.wsn AND z_4.fid = 4
    LEFT OUTER JOIN zdata z_5 ON w.wsn = z_5.wsn AND z_5.fid = 5
    LEFT OUTER JOIN zdata z_6 ON w.wsn = z_6.wsn AND z_6.fid = 6
    LEFT OUTER JOIN zdata z_7 ON w.wsn = z_7.wsn AND z_7.fid = 7
    LEFT OUTER JOIN zdata z_8 ON w.wsn = z_8.wsn AND z_8.fid = 8
    LEFT OUTER JOIN zdata z_9 ON w.wsn = z_9.wsn AND z_9.fid = 9
    LEFT OUTER JOIN zdata z_10 ON w.wsn = z_10.wsn AND z_10.fid = 10
    LEFT OUTER JOIN zdata z_11 ON w.wsn = z_11.wsn AND z_11.fid = 11
    LEFT OUTER JOIN zdata z_12 ON w.wsn = z_12.wsn AND z_12.fid = 12
    LEFT OUTER JOIN zdata z_13 ON w.wsn = z_13.wsn AND z_13.fid = 13
    LEFT OUTER JOIN zdata z_14 ON w.wsn = z_14.wsn AND z_14.fid = 14
    LEFT OUTER JOIN zdata z_15 ON w.wsn = z_15.wsn AND z_15.fid = 15
    LEFT OUTER JOIN zdata z_16 ON w.wsn = z_16.wsn AND z_16.fid = 16
    LEFT OUTER JOIN zdata z_17 ON w.wsn = z_17.wsn AND z_17.fid = 17
    LEFT OUTER JOIN zdata z_18 ON w.wsn = z_18.wsn AND z_18.fid = 18
    LEFT OUTER JOIN zdata z_19 ON w.wsn = z_19.wsn AND z_19.fid = 19
    LEFT OUTER JOIN zflddef f ON f.zid = 2 AND f.fid = w.elev_zid
    LEFT OUTER JOIN zdata z_20 ON w.wsn = z_20.wsn AND z_20.fid = w.elev_fid
    {purr_where}
    """,
    "identifier": f"""
    SELECT
      LIST({id_form}) as keylist
    FROM well w
    LEFT JOIN locat s ON s.wsn = w.wsn
    LEFT JOIN bhloc b ON b.wsn = w.wsn
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT OUTER JOIN zdata z_1 ON w.wsn = z_1.wsn AND z_1.fid = 1
    LEFT OUTER JOIN zdata z_2 ON w.wsn = z_2.wsn AND z_2.fid = 2
    LEFT OUTER JOIN zdata z_3 ON w.wsn = z_3.wsn AND z_3.fid = 3
    LEFT OUTER JOIN zdata z_4 ON w.wsn = z_4.wsn AND z_4.fid = 4
    LEFT OUTER JOIN zdata z_5 ON w.wsn = z_5.wsn AND z_5.fid = 5
    LEFT OUTER JOIN zdata z_6 ON w.wsn = z_6.wsn AND z_6.fid = 6
    LEFT OUTER JOIN zdata z_7 ON w.wsn = z_7.wsn AND z_7.fid = 7
    LEFT OUTER JOIN zdata z_8 ON w.wsn = z_8.wsn AND z_8.fid = 8
    LEFT OUTER JOIN zdata z_9 ON w.wsn = z_9.wsn AND z_9.fid = 9
    LEFT OUTER JOIN zdata z_10 ON w.wsn = z_10.wsn AND z_10.fid = 10
    LEFT OUTER JOIN zdata z_11 ON w.wsn = z_11.wsn AND z_11.fid = 11
    LEFT OUTER JOIN zdata z_12 ON w.wsn = z_12.wsn AND z_12.fid = 12
    LEFT OUTER JOIN zdata z_13 ON w.wsn = z_13.wsn AND z_13.fid = 13
    LEFT OUTER JOIN zdata z_14 ON w.wsn = z_14.wsn AND z_14.fid = 14
    LEFT OUTER JOIN zdata z_15 ON w.wsn = z_15.wsn AND z_15.fid = 15
    LEFT OUTER JOIN zdata z_16 ON w.wsn = z_16.wsn AND z_16.fid = 16
    LEFT OUTER JOIN zdata z_17 ON w.wsn = z_17.wsn AND z_17.fid = 17
    LEFT OUTER JOIN zdata z_18 ON w.wsn = z_18.wsn AND z_18.fid = 18
    LEFT OUTER JOIN zdata z_19 ON w.wsn = z_19.wsn AND z_19.fid = 19
    LEFT OUTER JOIN zflddef f ON f.zid = 2 AND f.fid = w.elev_zid
    LEFT OUTER JOIN zdata adv ON w.wsn = adv.wsn AND adv.fid = w.elev_fid
    {purr_where}
    """,
    "prefixes": {
        "w_": "well",
        "s_": "locat",
        "b_": "bhloc",
        "z_": "zdata",
        "u_": "uwi",
        "f_": "zflddef",
    },
    "identifier_keys": identifier_keys,
    "purr_where": purr_where,
    "xforms": {
        "w_adddate": "excel_date",
        "w_chgdate": "excel_date",
        "w_remarks": "memo_to_string",
        "s_congress": "parse_congressional",
        "s_congress_orig": "blob_to_hex",
        "s_texasloc": "blob_to_hex",
        "s_offshore": "blob_to_hex",
        "b_congress": "parse_congressional",
        "b_congress_orig": "blob_to_hex",
        "b_texasloc": "blob_to_hex",
        "b_offshore": "blob_to_hex",
        "z_comp_date": "excel_date",
        "z_spud_date": "excel_date",
        "z_permit_date": "excel_date",
        "z_rig_date": "excel_date",
        "z_aband_date": "excel_date",
        "z_report_date": "excel_date",
        "z_wrs_date": "excel_date",
        "z_last_act_date": "excel_date",
        "z_platform": "memo_to_string",
    },
}
