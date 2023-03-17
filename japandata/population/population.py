"""
population/data.py

Module which loads, caches, and provides access to population data

Author: Sam Passaglia
"""

import os
import tarfile
from pathlib import Path

import numpy as np
import pandas as pd

from japandata.utils import logger

CACHE_FOLDER = Path(Path(__file__).parent, "cache/")

"""
Data fetching and caching
"""


def fetch_data():
    """Fetches and caches data

    Returns:
        Path: cached filepath.
    """

    cached = Path(CACHE_FOLDER, "population/")
    archive = Path(CACHE_FOLDER, "indices.tar.gz")
    if not cached.exists():
        cached.parent.mkdir(parents=True, exist_ok=True)  # recreate any required subdirectories

        logger.info("Fetching data for japandata.population")

        from japandata.download import DOWNLOAD_INFO, download_progress

        url = DOWNLOAD_INFO["population"]["latest"]["url"]
        download_progress(url, archive)

        with tarfile.open(archive, "r") as tf:
            tf.extractall(cached.parent)
        os.remove(archive)
    return cached


DATA_FOLDER = fetch_data()

"""
"""


def load_age_year(year, datalevel="prefecture", poptype="resident"):
    assert datalevel in ["prefecture", "city"]
    assert poptype in ["resident", "japanese", "non-japanese"]
    logger.info(f"Processing age data for {year} {datalevel} {poptype}")

    fileextension = ".xls"
    skiprows = 2
    if year >= 2021:
        fileextension = ".xlsx"
        skiprows = 3

    forced_coltypes = {"code6digit": str, "prefecture": str}
    cols = ["code6digit", "prefecture"]
    if datalevel == "city":
        cols += ["city"]
    cols += ["gender", "total-pop"]
    if year == 2005:
        cols += ["total-pop-corrected"]
    agebracketmin = 0
    if year < 2015:
        agebracketmax = 80
    else:
        agebracketmax = 100
    while agebracketmin < agebracketmax:
        cols += [(str(int(agebracketmin)) + "-" + str(agebracketmin + 4))]
        agebracketmin += 5
    cols += [">" + str(int(agebracketmax - 1))]

    if datalevel == "prefecture":
        if poptype == "resident":
            filelabel = str(year)[-2:] + "02"
            if year >= 2013:
                filelabel += "s"
        elif poptype == "japanese":
            filelabel = str(year)[-2:] + "06n"
        elif poptype == "non-japanese":
            filelabel = str(year)[-2:] + "10g"

        df = pd.read_excel(
            Path(DATA_FOLDER, "tnen", filelabel + "tnen" + fileextension),
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )
    elif datalevel == "city":
        if poptype == "resident":
            filelabel = str(year)[-2:] + "04"
            if year >= 2013:
                filelabel += "s"
        elif poptype == "japanese":
            filelabel = str(year)[-2:] + "08n"
        elif poptype == "non-japanese":
            filelabel = str(year)[-2:] + "12g"
        df = pd.read_excel(
            Path(DATA_FOLDER, "snen", filelabel + "snen" + fileextension),
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )

    if (year >= 2021) and (poptype != "japanese"):
        df = df[:-2]

    if datalevel == "city":
        df["city"].replace("\x1f", np.nan, inplace=True)
        df["city"].replace("-", np.nan, inplace=True)
        df["city"] = df["city"].str.strip()
        df["city"] = df["city"].str.replace("*", "", regex=False)
        df["city"].replace("", np.nan, inplace=True)

    df.loc[:, ~df.columns.isin(["city", "code"])] = df.loc[
        :, ~df.columns.isin(["city", "code"])
    ].fillna(0)

    df = df.replace("X", 0)

    df["prefecture"] = df["prefecture"].str.strip()
    df["prefecture"] = df["prefecture"].str.replace("*", "", regex=False)

    df.loc[df["prefecture"] == "合計", "code6digit"] = np.nan

    if datalevel == "city":
        df.loc[df["city"] == "島しょ", "code6digit"] = "133604"
        df.loc[df["city"] == "色丹郡色丹村", "code6digit"] = "016951"
        df.loc[df["city"] == "国後郡泊村", "code6digit"] = "016969"
        df.loc[df["city"] == "国後郡留夜別村", "code6digit"] = "016977"
        df.loc[df["city"] == "択捉郡留別村", "code6digit"] = "016985"
        df.loc[df["city"] == "紗那郡紗那村", "code6digit"] = "016993"
        df.loc[df["city"] == "蘂取郡蘂取村", "code6digit"] = "017001"

    if datalevel == "prefecture":
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:2])
        df.drop(["code6digit"], inplace=True, axis=1)
    if datalevel == "city":
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])

    if year == 2005:
        df = df.drop("total-pop-corrected", axis=1)

    # SELF-CONSISTENCY TESTS #
    # men+women = total
    grouped = df.drop(
        ["code6digit", "prefecture", "city", "gender"],
        axis=1,
        errors="ignore",
    ).groupby("code")

    def testfunc(group):
        # print(group)
        assert (
            group.iloc[0, :-1] == group.iloc[1, :-1] + group.iloc[2, :-1]
        ).all()  # TODO this probably needs to be fixed

    grouped.apply(testfunc)
    # SELF-CONSISTENCY TESTS #

    df["unknown"] = df["total-pop"] - df.drop(
        [
            "code",
            "code6digit",
            "prefecture",
            "city",
            "gender",
            "total-pop",
        ],
        axis=1,
        errors="ignore",
    ).sum(axis=1)

    df["gender"].replace({"計": "total", "男": "men", "女": "women"}, inplace=True)

    df["year"] = year

    if poptype == "resident":
        # before the 2013 table, residents means japanese
        if year < 2013:
            nationality = "japanese"
        else:
            nationality = "all"
    else:
        nationality = poptype

    df["nationality"] = nationality

    # Clean up column order
    sorted_cols = ["year", "nationality"]
    df = df.reindex(columns=(sorted_cols + list([a for a in df.columns if a not in sorted_cols])))

    # Clean up types
    type_dict = {
        col: int
        for col in df.columns
        if col
        not in [
            "year",
            "gender",
            "nationality",
            "code6digit",
            "code",
            "prefecture",
            "city",
        ]
    }
    for key in type_dict.keys():
        if key in df.columns:
            df[key] = df[key].astype(type_dict[key])

    return df


def load_pop_year(year, datalevel="prefecture", poptype="resident"):
    assert datalevel in ["prefecture", "city"]
    assert poptype in ["resident", "japanese", "non-japanese"]

    logger.info(f"Processing pop data for {year} {datalevel} {poptype}")
    fileextension = ".xls"
    skiprows = 4
    if year >= 2021:
        fileextension = ".xlsx"
        skiprows = 6

    forced_coltypes = {"code6digit": str, "prefecture": str, "city": str}

    cols = ["code6digit", "prefecture"]
    if datalevel == "city":
        cols += ["city"]
    cols += ["men", "women", "total-pop"]
    if year == 2005:
        cols += ["total-pop-corrected"]
    if poptype == "japanese":
        cols += ["households-singlecitizenship", "households-multicitizenship"]
    cols += ["households"]
    if year == 2005:
        cols += ["households-corrected"]
    if 1980 <= year:
        if 2013 <= year:
            cols += ["moved-in-domestic", "moved-in-international"]
        cols += ["moved-in", "births"]
        if poptype == "japanese":
            cols += ["naturalization", "other-in-other"]
        if poptype == "non-japanese":
            if year == 2013:
                cols += ["other-30-47"]
            cols += ["denaturalization", "other-in-other"]
        cols += ["other-in", "total-in"]
        if 2013 <= year:
            cols += ["moved-out-domestic", "moved-out-international"]
        cols += ["moved-out", "deaths"]
        if poptype == "japanese":
            cols += ["denaturalization", "other-out-other"]
        if poptype == "non-japanese":
            cols += ["naturalization", "other-out-other"]
        cols += ["other-out", "total-out", "in-minus-out"]
        if 1994 <= year:
            cols += ["in-minus-out-rate"]
        cols += [
            "births-minus-deaths",
            "births-minus-deaths-rate",
            "social-in-minus-social-out",
            "social-in-minus-social-out-rate",
        ]

    if datalevel == "prefecture":
        if poptype == "resident":
            filelabel = str(year)[-2:] + "01"
            if year >= 2013:
                filelabel += "s"
        elif poptype == "japanese":
            filelabel = str(year)[-2:] + "05n"
        elif poptype == "non-japanese":
            filelabel = str(year)[-2:] + "09g"
        df = pd.read_excel(
            Path(DATA_FOLDER, "tjin", filelabel + "tjin" + fileextension),
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )
    elif datalevel == "city":
        if poptype == "resident":
            filelabel = str(year)[-2:] + "03"
            if year >= 2013:
                filelabel += "s"
        elif poptype == "japanese":
            filelabel = str(year)[-2:] + "07n"
        elif poptype == "non-japanese":
            filelabel = str(year)[-2:] + "11g"
        df = pd.read_excel(
            Path(DATA_FOLDER, "sjin", filelabel + "sjin" + fileextension),
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )

    df = df.drop(
        [
            "total-pop-corrected",
            "households-corrected",
            "in-minus-out-rate",
            "births-minus-deaths-rate",
            "social-in-minus-social-out-rate",
        ],
        axis=1,
        errors="ignore",
    )

    if year >= 2021:
        df = df[:-1]

    df["prefecture"] = df["prefecture"].str.strip()
    df.loc[df["prefecture"] == "合計", "code6digit"] = np.nan

    if datalevel == "city":
        df["city"].replace("\x1f", np.nan, inplace=True)
        df["city"].replace("-", np.nan, inplace=True)
        df["city"] = df["city"].str.strip()
        df.loc[df["city"] == "島しょ", "code6digit"] = "133604"
        df.loc[df["city"] == "色丹郡色丹村", "code6digit"] = "016951"
        df.loc[df["city"] == "国後郡泊村", "code6digit"] = "016969"
        df.loc[df["city"] == "国後郡留夜別村", "code6digit"] = "016977"
        df.loc[df["city"] == "択捉郡留別村", "code6digit"] = "016985"
        df.loc[df["city"] == "紗那郡紗那村", "code6digit"] = "016993"
        df.loc[df["city"] == "蘂取郡蘂取村", "code6digit"] = "017001"

    if datalevel == "prefecture":
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:2])
        df.drop(["code6digit"], inplace=True, axis=1)
    if datalevel == "city":
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])

    # SELF-CONSISTENCY TESTS #
    assert (df["men"] + df["women"] == df["total-pop"]).all()
    if year >= 1980:
        assert (df["moved-in"] + df["births"] + df["other-in"] == df["total-in"]).all()
        if year != 1996 and datalevel != "city":
            assert (df["moved-out"] + df["deaths"] + df["other-out"] == df["total-out"]).all()
        assert (df["total-in"] - df["total-out"] == df["in-minus-out"]).all()
        assert (df["births"] - df["deaths"] == df["births-minus-deaths"]).all()
        assert (
            df["moved-in"] + df["other-in"] - df["moved-out"] - df["other-out"]
            == df["social-in-minus-social-out"]
        ).all()
    if year >= 2013:
        assert (df["moved-in-domestic"] + df["moved-in-international"] == df["moved-in"]).all()
        assert (df["moved-out-domestic"] + df["moved-out-international"] == df["moved-out"]).all()
    if datalevel == "prefecture":
        assert (
            df.drop(df.loc[df["prefecture"] == "合計"].index)
            .drop(
                [
                    "code6digit",
                    "code",
                    "prefecture",
                ],
                axis=1,
                errors="ignore",
            )
            .sum()
            .values
            == df.loc[df["prefecture"] == "合計"]
            .drop(
                [
                    "code6digit",
                    "code",
                    "prefecture",
                ],
                axis=1,
                errors="ignore",
            )
            .values
        ).all()
    # SELF-CONSISTENCY TESTS #

    df["year"] = year
    if poptype == "resident":
        # before the 2013 table, residents means japanese
        if year < 2013:
            nationality = "japanese"
        else:
            nationality = "all"
    else:
        nationality = poptype

    df["nationality"] = nationality

    # Clean up column order
    sorted_cols = ["year", "nationality"]
    df = df.reindex(columns=(sorted_cols + list([a for a in df.columns if a not in sorted_cols])))

    # Clean up types
    type_dict = {
        col: int
        for col in df.columns
        if col
        not in [
            "year",
            "nationality",
            "code6digit",
            "code",
            "prefecture",
            "city",
        ]
    }
    for key in type_dict.keys():
        if key in df.columns:
            df[key] = df[key].astype(type_dict[key])

    return df


def load_pop():
    poptypes = ["resident", "japanese", "non-japanese"]

    complete_japan_df = pd.DataFrame()
    complete_pref_df = pd.DataFrame()
    complete_city_df = pd.DataFrame()

    for poptype in poptypes:
        if poptype == "resident":
            years = np.arange(1968, 2023)
        else:
            years = np.arange(2013, 2023)

        japan_df = pd.DataFrame()
        pref_df = pd.DataFrame()
        city_df = pd.DataFrame()
        for year in years:
            pref_df_year = load_pop_year(year, poptype=poptype, datalevel="prefecture")
            japan_df_year = (
                pref_df_year[pref_df_year["prefecture"] == "合計"]
                .copy()
                .drop(
                    ["prefecture", "code"],
                    axis=1,
                    errors="ignore",
                )
            )
            pref_df_year.drop(
                pref_df_year.loc[pref_df_year["prefecture"] == "合計"].index, inplace=True
            )

            japan_df = pd.concat([japan_df, japan_df_year], ignore_index=True)
            pref_df = pd.concat([pref_df, pref_df_year], ignore_index=True)

            if year >= 1995:
                city_df_year = load_pop_year(year, poptype=poptype, datalevel="city")

                # the summary rows of the city table
                city_df_year_prefrows = (
                    city_df_year.loc[pd.isna(city_df_year["city"])]
                    .drop(["city", "code", "code6digit", "prefecture"], axis=1)
                    .reset_index(drop=True)
                )
                # checking consistency of the japan table and the city table
                assert (japan_df_year.values == city_df_year_prefrows.iloc[0].values).all()
                # checking consistency of the prefecture table and the city table
                assert (
                    pref_df_year.drop(
                        [
                            "code",
                            "prefecture",
                        ],
                        axis=1,
                    ).values
                    == city_df_year_prefrows.iloc[1:].values
                ).all()

                # dropping the summary rows
                city_df_year = city_df_year.loc[~pd.isna(city_df_year["city"])]

                city_df = pd.concat([city_df, city_df_year], ignore_index=True)

        complete_japan_df = pd.concat([complete_japan_df, japan_df], ignore_index=True)
        complete_pref_df = pd.concat([complete_pref_df, pref_df], ignore_index=True)
        complete_city_df = pd.concat([complete_city_df, city_df], ignore_index=True)

    # dropping columns that I don't have confidence in
    complete_japan_df = complete_japan_df.drop(
        [
            "moved-in",
            "other-in",
            "total-in",
            "moved-out",
            "other-out",
            "total-out",
            "in-minus-out",
            "moved-in-domestic",
            "moved-out-domestic",
            "moved-in-international",
            "moved-out-international",
            "naturalization",
            "other-in-other",
            "denaturalization",
            "other-out-other",
            "other-30-47",
        ],
        axis=1,
    )

    # Until now, population in a given year means the value at the beginning of the year and the population flows are the flows in the previous year.
    # Here we subtract 1 and so we have the population at the end of the year and the flows in the year.
    complete_japan_df["year"] = complete_japan_df["year"] - 1
    complete_pref_df["year"] = complete_pref_df["year"] - 1
    complete_city_df["year"] = complete_city_df["year"] - 1

    return complete_japan_df, complete_pref_df, complete_city_df


def load_age():
    poptypes = ["resident", "japanese", "non-japanese"]

    complete_japan_df = pd.DataFrame()
    complete_pref_df = pd.DataFrame()
    complete_city_df = pd.DataFrame()

    for poptype in poptypes:
        if poptype == "resident":
            years = np.arange(1994, 2023)
        else:
            years = np.arange(2013, 2023)

        japan_df = pd.DataFrame()
        pref_df = pd.DataFrame()
        city_df = pd.DataFrame()
        for year in years:
            pref_df_year = load_age_year(year, poptype=poptype, datalevel="prefecture")
            japan_df_year = (
                pref_df_year[pref_df_year["prefecture"] == "合計"]
                .copy()
                .reset_index(drop=True)
                .drop(["code", "prefecture"], axis=1)
            )
            pref_df_year.drop(
                pref_df_year.loc[pref_df_year["prefecture"] == "合計"].index, inplace=True
            )

            japan_df = pd.concat([japan_df, japan_df_year], ignore_index=True)
            pref_df = pd.concat([pref_df, pref_df_year], ignore_index=True)

            if year >= 1995:
                city_df_year = load_age_year(year, poptype=poptype, datalevel="city")

                # the summary rows of the city table
                city_df_year_prefrows = (
                    city_df_year.loc[pd.isna(city_df_year["city"])]
                    .drop(["city", "code", "code6digit", "prefecture"], axis=1)
                    .reset_index(drop=True)
                )

                # checking consistency of the japan table and the city table
                assert (japan_df_year.values == city_df_year_prefrows.iloc[0:3].values).all()

                # checking consistency of the prefecture table and the city table
                assert (
                    pref_df_year.drop(
                        [
                            "code",
                            "prefecture",
                        ],
                        axis=1,
                    ).values
                    == city_df_year_prefrows.iloc[3:].values
                ).all()

                # dropping the summary rows
                city_df_year = city_df_year.loc[~pd.isna(city_df_year["city"])]

                city_df = pd.concat([city_df, city_df_year], ignore_index=True)

        complete_japan_df = pd.concat([complete_japan_df, japan_df], ignore_index=True)
        complete_pref_df = pd.concat([complete_pref_df, pref_df], ignore_index=True)
        complete_city_df = pd.concat([complete_city_df, city_df], ignore_index=True)

    # Until now, population in a given year means the value at the beginning of the year and the population flows are the flows in the previous year.
    # Here we subtract 1 and so we have the population at the end of the year and the flows in the year.
    complete_japan_df["year"] = complete_japan_df["year"] - 1
    complete_pref_df["year"] = complete_pref_df["year"] - 1
    complete_city_df["year"] = complete_city_df["year"] - 1

    return complete_japan_df, complete_pref_df, complete_city_df


"""
Loading and caching of the cleaned data
"""


def fetch_dataframes():
    JAPAN_POP_CACHE = Path(CACHE_FOLDER, "japan_pop.parquet")
    JAPAN_AGE_CACHE = Path(CACHE_FOLDER, "japan_age.parquet")
    PREF_POP_CACHE = Path(CACHE_FOLDER, "pref_pop.parquet")
    PREF_AGE_CACHE = Path(CACHE_FOLDER, "pref_age.parquet")
    CITY_POP_CACHE = Path(CACHE_FOLDER, "city_pop.parquet")
    CITY_AGE_CACHE = Path(CACHE_FOLDER, "city_age.parquet")

    if not (
        JAPAN_POP_CACHE.exists()
        and JAPAN_AGE_CACHE.exists()
        and PREF_POP_CACHE.exists()
        and PREF_AGE_CACHE.exists()
        and CITY_POP_CACHE.exists()
        and CITY_AGE_CACHE.exists()
    ):
        japan_age, pref_age, city_age = load_age()
        japan_pop, pref_pop, city_pop = load_pop()

        japan_pop.to_parquet(JAPAN_POP_CACHE)
        japan_age.to_parquet(JAPAN_AGE_CACHE)
        pref_pop.to_parquet(PREF_POP_CACHE)
        pref_age.to_parquet(PREF_AGE_CACHE)
        city_pop.to_parquet(CITY_POP_CACHE)
        city_age.to_parquet(CITY_AGE_CACHE)

    japan_pop = pd.read_parquet(JAPAN_POP_CACHE)
    japan_age = pd.read_parquet(JAPAN_AGE_CACHE)
    pref_pop = pd.read_parquet(PREF_POP_CACHE)
    pref_age = pd.read_parquet(PREF_AGE_CACHE)
    city_pop = pd.read_parquet(CITY_POP_CACHE)
    city_age = pd.read_parquet(CITY_AGE_CACHE)

    return japan_pop, japan_age, pref_pop, pref_age, city_pop, city_age


japan_pop, japan_age, pref_pop, pref_age, city_pop, city_age = fetch_dataframes()
