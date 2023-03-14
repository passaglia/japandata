"""
indices/indices.py

Module which provides fiscal health indices about Japanese municipalities
Data from https://www.soumu.go.jp/iken/shihyo_ichiran.html

Author: Sam Passaglia
"""

import os
import tarfile
from pathlib import Path

import numpy as np
import pandas as pd

from japandata.maps import load_map
from japandata.utils import japanese_to_western, western_to_japanese

CACHE_FOLDER = Path(Path(__file__).parent, "cache/")

"""
Data fetching and caching
"""


def fetch_data():
    """Fetches and caches data

    Returns:
        Path: cached filepath.
    """

    cached = Path(CACHE_FOLDER, "indices/")
    archive = Path(CACHE_FOLDER, "indices.tar.gz")
    if not cached.exists():
        cached.parent.mkdir(
            parents=True, exist_ok=True
        )  # recreate any required subdirectories cityly

        from japandata.download import DOWNLOAD_INFO, download_progress

        url = DOWNLOAD_INFO["indices"]["latest"]["url"]
        download_progress(url, archive)

        with tarfile.open(archive, "r") as tf:
            tf.extractall(cached.parent)
        os.remove(archive)
    return cached


DATA_FOLDER = fetch_data()

"""
Data Processing
"""


def load_year(year, scale="prefecture"):
    """Loads data for a given year and scale

    Args:
        year (int): year to load
        scale (str, optional): scale to load. Defaults to "prefecture".
                                   Expect "prefecture", "city", "prefecturemean",    "designatedcity", "capital",

    Returns:
        pd.DataFrame: cleaned data
    """

    extension = ".xls"
    skiprows = 2
    if year >= 2016:
        extension = ".xlsx"

    forced_coltypes = {"code6digit": str, "prefecture": str}
    cols = []
    if (year >= 2011) and (scale == "city"):
        cols += ["code6digit"]
    cols += ["prefecture"]
    if scale == "prefecturemean":
        cols += ["useless"]
    if scale in ["city", "designatedcity", "capital"]:
        cols += ["city"]
    if year <= 2007:
        cols += [
            "regular-expense-rate",
            "debt-service-rate",
            "debt-restriction-rate",
            "fiscal-strength-index",
        ]
    else:
        cols += [
            "fiscal-strength-index",
            "regular-expense-rate",
            "debt-service-rate",
            "future-burden-rate",
        ]
    if not ((scale == "prefecturemean") and (year == 2007)):
        cols += ["laspeyres"]
    if (year == 2012 or year == 2013) and (scale != "prefecturemean"):
        cols += ["laspeyres-adjusted"]

    filelabel = western_to_japanese(year)

    df = pd.read_excel(
        Path(DATA_FOLDER, scale, filelabel + extension),
        skiprows=skiprows,
        header=None,
        names=cols,
        dtype=forced_coltypes,
    )

    df["prefecture"] = df["prefecture"].str.strip()
    df.drop(df[df["prefecture"] == "都道府県平均"].index, inplace=True)
    df.drop(df[df["prefecture"] == "全国市町村"].index, inplace=True)
    df.drop(df[df["prefecture"] == "全国市町村平均"].index, inplace=True)
    df.drop(df[df["prefecture"] == "政令指定都市平均"].index, inplace=True)
    df.drop(df[df["prefecture"] == "道府県庁所在市平均"].index, inplace=True)
    df.drop(df[df["prefecture"] == "道府県庁所在市"].index, inplace=True)
    df.drop(df[df["prefecture"] == "政令指定都市"].index, inplace=True)
    df.drop(df[pd.isna(df["debt-service-rate"])].index, inplace=True)

    if scale == "prefecturemean":
        df.drop("useless", axis=1, errors="ignore", inplace=True)
        df.drop("laspeyres", axis=1, errors="ignore", inplace=True)

    df.drop("laspeyres-adjusted", axis=1, errors="ignore", inplace=True)

    if scale == "city":
        df.drop(df.loc[pd.isna(df["city"])].index, inplace=True)
        df = df.replace({r"\(": "", r"\)": ""}, regex=True)

    if scale == "city" and year >= 2011:
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])
        df.drop("code6digit", axis=1, inplace=True)

    df.replace("-", np.nan, inplace=True)
    df.replace("－", np.nan, inplace=True)

    float_cols = [
        "future-burden-rate",
        "laspeyres",
        "debt-restriction-rate",
        "regular-expense-rate",
        "debt-service-rate",
        "fiscal-strength-index",
        "laspeyres-adjusted",
    ]
    for col in float_cols:
        if col in df.columns:
            df = df.astype({col: np.float64})

    if scale == "prefecture":
        assert len(df) == 47
        assert len(df.columns) == 6
    elif scale == "prefecturemean":
        assert len(df) == 47
        assert len(df.columns) == 5
    elif scale == "city":
        assert len(df["prefecture"].unique()) == 47
        if year < 2011:
            assert len(df.columns) == 7
        else:
            assert len(df.columns) == 8
    elif scale == "capital":
        assert len(df["prefecture"].unique()) == 47

    df["year"] = year

    return df


def load_all():
    """Loads all data from the data folder"""

    files = Path(DATA_FOLDER, "city").glob("*")
    years = [japanese_to_western(file.name.split(".")[0]) for file in files]
    years.sort()

    df_pref_list = []
    df_prefmean_list = []
    df_city_list = []
    df_designatedcity_list = []
    df_capital_list = []
    for year in years:
        print(year)
        df_pref = load_year(year, "prefecture")
        df_prefmean = load_year(year, "prefecturemean")
        df_city = load_year(year, "city")
        df_designatedcity = load_year(year, "designatedcity")
        df_capital = load_year(year, "capital")

        # In early years the codes were not listed. Fetch them from the maps.
        if year < 2011:
            map_df = load_map(year + 2)
            alt_map_df = load_map(year - 2)
            df_city["code"] = np.nan

            def findCode(row):
                pref = row["prefecture"]
                city = row["city"].replace("ケ", "ヶ")
                potentialcities = [
                    city,
                    city.replace("ケ", "ヶ"),
                    city.replace("ヶ", "ケ"),
                    city.replace("桧", "檜"),
                    city.replace("竜", "龍"),
                    city.replace("曾", "曽"),
                ]
                try:
                    codefound = map_df.loc[
                        (
                            map_df["city"].isin(potentialcities)
                            | map_df["county"].isin(potentialcities)
                        )
                        & (map_df["prefecture"] == pref),
                        "code",
                    ].values[0]
                except IndexError:
                    codefound = alt_map_df.loc[
                        (
                            alt_map_df["city"].isin(potentialcities)
                            | alt_map_df["county"].isin(potentialcities)
                        )
                        & (alt_map_df["prefecture"] == pref),
                        "code",
                    ].values[0]
                return codefound

            df_city["code"] = df_city.apply(findCode, axis=1)

        df_pref_list.append(df_pref)
        df_prefmean_list.append(df_prefmean)
        df_city_list.append(df_city)
        df_designatedcity_list.append(df_designatedcity)
        df_capital_list.append(df_capital)

    df_pref = pd.concat(df_pref_list, axis=0, join="outer", ignore_index=True)
    df_prefmean = pd.concat(df_prefmean_list, axis=0, join="outer", ignore_index=True)
    df_city = pd.concat(df_city_list, axis=0, join="outer", ignore_index=True)
    df_designatedcity = pd.concat(
        df_designatedcity_list, axis=0, join="outer", ignore_index=True
    )
    df_capital = pd.concat(df_capital_list, axis=0, join="outer", ignore_index=True)

    return (df_pref, df_prefmean, df_city, df_designatedcity, df_capital)


"""
Loading and caching of the cleaned data
"""


def fetch_dataframes():
    PREF_CACHE = Path(CACHE_FOLDER, "pref.parquet")
    PREFMEAN_CACHE = Path(CACHE_FOLDER, "prefmean.parquet")
    DESIGNATEDCITY_CACHE = Path(CACHE_FOLDER, "designatedcity.parquet")
    CAPITAL_CACHE = Path(CACHE_FOLDER, "capital.parquet")
    CITY_CACHE = Path(CACHE_FOLDER, "city.parquet")

    if not (
        PREF_CACHE.exists()
        and PREFMEAN_CACHE.exists()
        and DESIGNATEDCITY_CACHE.exists()
        and CAPITAL_CACHE.exists()
        and CITY_CACHE.exists()
    ):
        (df_pref, df_prefmean, df_city, df_designatedcity, df_capital) = load_all()
        df_pref.to_parquet(PREF_CACHE)
        df_prefmean.to_parquet(PREFMEAN_CACHE)
        df_city.to_parquet(CITY_CACHE)
        df_designatedcity.to_parquet(DESIGNATEDCITY_CACHE)
        df_capital.to_parquet(CAPITAL_CACHE)

    df_pref = pd.read_parquet(PREF_CACHE)
    df_prefmean = pd.read_parquet(PREFMEAN_CACHE)
    df_city = pd.read_parquet(CITY_CACHE)
    df_designatedcity = pd.read_parquet(DESIGNATEDCITY_CACHE)
    df_capital = pd.read_parquet(CAPITAL_CACHE)
    return (df_pref, df_prefmean, df_city, df_designatedcity, df_capital)


pref, prefmean, city, designatedcity, capital = fetch_dataframes()
