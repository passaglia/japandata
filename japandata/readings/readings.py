"""
readings/data.py

Module which provides readings for place names

Author: Sam Passaglia
"""

import os
from pathlib import Path

import jaconv
import pandas as pd
import romkan

CACHE_FOLDER = Path(os.path.dirname(__file__), "cache/")
os.makedirs(CACHE_FOLDER, exist_ok=True)


def fetch_readings_file():
    """Fetches and caches file

    Returns:
        Path: cached filepath.
    """

    cached = Path(CACHE_FOLDER, "R2_loss.xlsx")
    if not cached.exists():
        cached.parent.mkdir(
            parents=True, exist_ok=True
        )  # recreate any required subdirectories locally

        from japandata.download import DOWNLOAD_INFO, download_progress

        url = DOWNLOAD_INFO["readings"]["latest"]["url"]
        download_progress(url, cached)
    return cached


def load_readings_R2file(fpath):
    colnames = ["code6digit", "prefecture", "city", "prefecture-kana", "city-kana"]

    df = pd.read_excel(fpath, names=colnames, dtype={"code6digit": str})
    df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])
    df.drop(["code6digit"], inplace=True, axis=1)

    prefecture_df = (
        df.loc[pd.isna(df["city"])]
        .drop(["city", "city-kana"], axis=1)
        .reset_index(drop=True)
    )
    prefecture_df["code"] = prefecture_df["code"].apply(lambda s: s[0:2])
    prefecture_df["prefecture-romaji"] = (
        prefecture_df["prefecture-kana"]
        .apply(lambda s: romkan.to_roma(jaconv.h2z(s).strip("ケン")))
        .str.replace("osakafu", "osaka")
        .str.replace("toukyouto", "toukyou")
        .str.replace("kyoutofu", "kyouto")
    )

    df = df.loc[~pd.isna(df["city"])].reset_index(drop=True)
    df["prefecture-romaji"] = (
        df["prefecture-kana"]
        .apply(lambda s: romkan.to_roma(jaconv.h2z(s).strip("ケン")))
        .str.replace("osakafu", "osaka")
        .str.replace("toukyouto", "toukyou")
        .str.replace("kyoutofu", "kyouto")
    )
    df["city-romaji"] = (
        df["city-kana"]
        .apply(lambda s: romkan.to_roma(jaconv.h2z(s)))
        .str.replace("du", "zu")
    )

    def stripper(row):
        if row.city[-1] == "市":
            return row["city-romaji"].removesuffix("shi")
        elif row.city[-1] == "町":
            stripped = row["city-romaji"].removesuffix("chou")
            if stripped == row["city-romaji"]:
                stripped = row["city-romaji"].removesuffix("machi")
            return stripped
        elif row.city[-1] == "村":
            stripped = row["city-romaji"].removesuffix("son")
            if stripped == row["city-romaji"]:
                stripped = row["city-romaji"].removesuffix("mura")
            return stripped
        else:
            return row["city-romaji"]

    df["city-romaji"] = df.apply(stripper, axis=1)

    return df, prefecture_df


city_names, pref_names = load_readings_R2file(fetch_readings_file())
