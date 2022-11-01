"""
indices/data.py

Module which loads, caches, and provides access to indices data

Author: Sam Passaglia
"""

import pandas as pd
import numpy as np
import xarray as xr
import os
from japandata.maps.data import load_map

# TODO: Remove the xarray dep

DATA_URL = (
    "https://github.com/passaglia/japandata-sources/raw/main/indices/indicesdata.tar.gz"
)
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "indicesdata/")

PREF_CACHE = os.path.join(os.path.dirname(__file__), "pref.parquet")
PREFMEAN_CACHE = os.path.join(os.path.dirname(__file__), "prefmean.parquet")
DESIGNATEDCITY_CACHE = os.path.join(os.path.dirname(__file__), "designatedcity.parquet")
CAPITAL_CACHE = os.path.join(os.path.dirname(__file__), "capital.parquet")
LOCAL_CACHE = os.path.join(os.path.dirname(__file__), "local.parquet")

## NOTE THAT SOME OF THE COLUMNS MEASURE DIFFERENT THINGS FOR TOKYO 23 THAN FOR OTHER LOCALITIES.


def checkfordata():
    return os.path.exists(DATA_FOLDER)


def getdata():
    if checkfordata():
        print("data already gotten")
        return
    else:
        import urllib.request
        import tarfile

        ftpstream = urllib.request.urlopen(DATA_URL)
        rawfile = tarfile.open(fileobj=ftpstream, mode="r|gz")
        rawfile.extractall(os.path.dirname(__file__))
        return


def load_data(year, datalevel="prefecture"):
    assert datalevel in [
        "prefecture",
        "local",
        "prefecturemean",
        "designatedcity",
        "capital",
    ]
    assert 2005 <= year <= 2020

    fileextension = ".xls"
    skiprows = 2
    if year >= 2016:
        fileextension = ".xlsx"

    forced_coltypes = {"code6digit": str, "prefecture": str}
    cols = []
    if (year >= 2011) and (datalevel == "local"):
        cols += ["code6digit"]
    cols += ["prefecture"]
    if datalevel == "prefecturemean":
        cols += ["useless"]
    if datalevel in ["local", "designatedcity", "capital"]:
        cols += ["city"]
    if year <= 2007:
        cols += [
            "regular-expense-rate",
            "debt-service-rate",
            "debt-restriction-rate",
            "economic-strength-index",
        ]
    else:
        cols += [
            "economic-strength-index",
            "regular-expense-rate",
            "debt-service-rate",
            "future-burden-rate",
        ]
    if not ((datalevel == "prefecturemean") and (year == 2007)):
        cols += ["laspeyres"]
    if (year == 2012 or year == 2013) and (datalevel != "prefecturemean"):
        cols += ["laspeyres-adjusted"]

    westernToLabel = (
        lambda year: "H" + str(year - 1988) if year < 2019 else "R" + str(year - 2018)
    )

    filelabel = westernToLabel(year)

    df = pd.read_excel(
        DATA_FOLDER + datalevel + "/" + filelabel + fileextension,
        skiprows=skiprows,
        header=None,
        names=cols,
        dtype=forced_coltypes,
    )

    df["prefecture"] = df["prefecture"].str.strip()
    df.drop(df.loc[df["prefecture"] == "都道府県平均"].index, inplace=True)
    df.drop(df.loc[df["prefecture"] == "全国市町村"].index, inplace=True)
    df.drop(df.loc[df["prefecture"] == "全国市町村平均"].index, inplace=True)
    df.drop(df.loc[df["prefecture"] == "政令指定都市平均"].index, inplace=True)
    df.drop(df.loc[df["prefecture"] == "道府県庁所在市平均"].index, inplace=True)
    df.drop(df.loc[df["prefecture"] == "道府県庁所在市"].index, inplace=True)
    df.drop(df.loc[df["prefecture"] == "政令指定都市"].index, inplace=True)
    df.drop(df.loc[pd.isna(df["debt-service-rate"])].index, inplace=True)

    if datalevel == "prefecturemean":
        df.drop("useless", axis=1, errors="ignore", inplace=True)
        df.drop("laspeyres", axis=1, errors="ignore", inplace=True)

    df.drop("laspeyres-adjusted", axis=1, errors="ignore", inplace=True)

    if datalevel == "local":
        df.drop(df.loc[pd.isna(df["city"])].index, inplace=True)
        df = df.replace({"\(": "", "\)": ""}, regex=True)

    if datalevel == "local" and year >= 2011:
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
        "economic-strength-index",
        "laspeyres-adjusted",
    ]
    for col in float_cols:
        if col in df.columns:
            df = df.astype({col: np.float64})

    return df


def clean_data():
    years = np.arange(2005, 2021)
    df_pref_list = []
    df_prefmean_list = []
    df_local_list = []
    df_designatedcity_list = []
    df_capital_list = []
    for year in years:
        print(year)
        df_pref = load_data(year, "prefecture")
        assert len(df_pref) == 47
        assert len(df_pref.columns) == 6
        df_prefmean = load_data(year, "prefecturemean")
        assert len(df_prefmean) == 47
        assert len(df_prefmean.columns) == 5
        df_local = load_data(year, "local")
        if year < 2011:
            assert len(df_local.columns) == 7
        else:
            assert len(df_local.columns) == 8
        assert len(df_local["prefecture"].unique()) == 47
        df_designatedcity = load_data(year, "designatedcity")
        df_capital = load_data(year, "capital")
        assert len(df_capital["prefecture"].unique()) == 47

        ## In early years the codes were not listed. Fetch them from the maps.
        if year < 2011:
            map_df = load_map(year + 2)
            alt_map_df = load_map(year - 2)
            df_local["code"] = np.nan

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

            df_local["code"] = df_local.apply(findCode, axis=1)

        # df_pref['year'] = year
        df_pref_list.append(df_pref)
        df_prefmean_list.append(df_prefmean)
        df_local_list.append(df_local)
        df_designatedcity_list.append(df_designatedcity)
        df_capital_list.append(df_capital)

    def unify_columns(df_list):
        cols = set()
        for df in df_list:
            cols = cols.union(df.columns)
        for df in df_list:
            for col in cols:
                if col not in df.columns:
                    df[col] = np.nan

    for df_list in [
        df_pref_list,
        df_prefmean_list,
        df_local_list,
        df_designatedcity_list,
        df_capital_list,
    ]:
        unify_columns(df_list)

    pref_array = xr.concat(
        [df.to_xarray() for df in df_pref_list], dim=xr.DataArray(years, dims="year")
    )
    prefmean_array = xr.concat(
        [df.to_xarray() for df in df_prefmean_list],
        dim=xr.DataArray(years, dims="year"),
    )
    local_array = xr.concat(
        [df.to_xarray() for df in df_local_list], dim=xr.DataArray(years, dims="year")
    )
    designatedcity_array = xr.concat(
        [df.to_xarray() for df in df_designatedcity_list],
        dim=xr.DataArray(years, dims="year"),
    )
    capital_array = xr.concat(
        [df.to_xarray() for df in df_capital_list], dim=xr.DataArray(years, dims="year")
    )

    return pref_array, prefmean_array, local_array, designatedcity_array, capital_array


try:
    pref_ind_xr = pd.read_parquet(PREF_CACHE).to_xarray()
    prefmean_ind_xr = pd.read_parquet(PREFMEAN_CACHE).to_xarray()
    local_ind_xr = pd.read_parquet(LOCAL_CACHE).to_xarray()
    designatedcity_ind_xr = pd.read_parquet(DESIGNATEDCITY_CACHE).to_xarray()
    capital_ind_xr = pd.read_parquet(CAPITAL_CACHE).to_xarray()
except FileNotFoundError:
    if not checkfordata():
        getdata()
    (
        pref_ind_xr,
        prefmean_ind_xr,
        local_ind_xr,
        designatedcity_ind_xr,
        capital_ind_xr,
    ) = clean_data()
    (pref_ind_xr.to_dataframe()).to_parquet(PREF_CACHE)
    (prefmean_ind_xr.to_dataframe()).to_parquet(PREFMEAN_CACHE)
    (local_ind_xr.to_dataframe()).to_parquet(LOCAL_CACHE)
    (designatedcity_ind_xr.to_dataframe()).to_parquet(DESIGNATEDCITY_CACHE)
    (capital_ind_xr.to_dataframe()).to_parquet(CAPITAL_CACHE)

pref_ind_df = (
    pref_ind_xr.to_dataframe().reset_index().drop("index", axis=1).fillna(value=np.nan)
)
prefmean_ind_df = (
    prefmean_ind_xr.to_dataframe()
    .reset_index()
    .drop("index", axis=1)
    .fillna(value=np.nan)
)
local_ind_df = (
    local_ind_xr.to_dataframe().reset_index().drop("index", axis=1).fillna(value=np.nan)
)
local_ind_df = local_ind_df.drop(local_ind_df.loc[pd.isna(local_ind_df["city"])].index)

designatedcity_ind_df = (
    designatedcity_ind_xr.to_dataframe()
    .reset_index()
    .drop("index", axis=1)
    .fillna(value=np.nan)
)
capital_ind_df = (
    capital_ind_xr.to_dataframe()
    .reset_index()
    .drop("index", axis=1)
    .fillna(value=np.nan)
)
