from pandas import DataFrame as df


def dst_agg(df):
    agg_columns = [col for col in df.columns if col.startswith("f_")]

    agg_dict = {
        col: (lambda x: [item for sublist in x for item in sublist])
        if col == "f_recov"
        else list
        for col in agg_columns
    }

    result = df.groupby("w_wsn", as_index=False).agg(agg_dict)

    return result


def ip_agg(df):
    agg_columns = [col for col in df.columns if col.startswith("p_")]

    agg_dict = {
        col: (lambda x: [item for sublist in x for item in sublist])
        if col == "p_treat"
        else list
        for col in agg_columns
    }
    print("^^^^^^^^^^^^???")
    print(agg_dict)
    print("^^^^^^^^^^^^???")

    result = df.groupby("w_wsn", as_index=False).agg(agg_dict)

    return result


post_process = {"dst_agg": dst_agg, "ip_agg": ip_agg}
