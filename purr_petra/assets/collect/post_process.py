from pandas import DataFrame as df
from typing import Any, List, Dict


def preserve_empty_lists(values: List[Any]) -> List[Any]:
    return [sublist if sublist is not None else [] for sublist in values]


# TODO: dry these up


def dst_agg(df):
    agg_columns = [col for col in df.columns if col.startswith("f_")]
    agg_dict = {
        col: preserve_empty_lists if col == "f_recov" else list for col in agg_columns
    }
    other_columns = [
        col for col in df.columns if col not in agg_columns and col != "w_wsn"
    ]
    for col in other_columns:
        agg_dict[col] = "first"

    return df.groupby("w_wsn", as_index=False).agg(agg_dict)


# 35137004570000
def ip_agg(df):
    agg_columns = [col for col in df.columns if col.startswith("p_")]
    agg_dict = {
        col: preserve_empty_lists if col == "p_treat" else list for col in agg_columns
    }

    other_columns = [
        col for col in df.columns if col not in agg_columns and col != "w_wsn"
    ]
    for col in other_columns:
        agg_dict[col] = "first"

    return df.groupby("w_wsn", as_index=False).agg(agg_dict)


post_process = {"dst_agg": dst_agg, "ip_agg": ip_agg}
