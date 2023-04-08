import hashlib

import pandas as pd


def surrogate_key(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Genenerates a surrogate key column by creating a hash of all
    provided column names."""

    dataframe["primary_key"] = ""

    for column in columns:

        dataframe["primary_key"] = dataframe[column].astype(str) + dataframe[
            "primary_key"
        ].astype(
            str
        )  # concat the rows of the provoided  unique columns into a single string

    dataframe["primary_key"] = dataframe["primary_key"].apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )  # md5 hash that string

    return dataframe
