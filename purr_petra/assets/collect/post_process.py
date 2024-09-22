import pandas as pd
from typing import Any, List

pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


def preserve_empty_lists(values: List[Any]) -> List[Any]:
    return [sublist if sublist is not None else [] for sublist in values]


def flexible_agg(
    df: pd.DataFrame, prefix_list: List[str], empty_list_cols: List[str] = []
) -> pd.DataFrame:
    def starts_with_any(col: str, prefixes: List[str]) -> bool:
        return any(col.startswith(prefix) for prefix in prefixes)

    agg_columns = [col for col in df.columns if starts_with_any(col, prefix_list)]

    agg_dict: dict[str, Any] = {}
    for col in agg_columns:
        if col in empty_list_cols:
            agg_dict[col] = preserve_empty_lists
        else:
            agg_dict[col] = list

    other_columns = [
        col for col in df.columns if col not in agg_columns and col != "w_wsn"
    ]
    for col in other_columns:
        agg_dict[col] = "first"

    return df.groupby("w_wsn", as_index=False).agg(agg_dict)


def dst_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["f_"],
        empty_list_cols=["f_recov"],
    )


def formation_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["f_", "z_", "t_"],
    )


def ip_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["p_"],
        empty_list_cols=["p_treat"],
    )


def perforation_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["p_"],
    )


def production_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["a_"],
    )


def raster_log_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["i_", "g_"],
    )


def vector_log_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["a_", "f_", "x_", "g_"],
    )


def zone_agg(df: pd.DataFrame) -> pd.DataFrame:
    return flexible_agg(
        df,
        prefix_list=["n_", "f_", "z_"],
    )


post_process = {
    "dst_agg": dst_agg,
    "formation_agg": formation_agg,
    "ip_agg": ip_agg,
    "perforation_agg": perforation_agg,
    "production_agg": production_agg,
    "raster_log_agg": raster_log_agg,
    "vector_log_agg": vector_log_agg,
    "zone_agg": zone_agg,
}
