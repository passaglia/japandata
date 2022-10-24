"""
population/data.py

Module which loads, caches, and provides access to population data

Author: Sam Passaglia
"""

import pandas as pd
import numpy as np
import os

## Figure out the new datastructure for the load all pop piece
## Implement japanese and foreigner in age
## Implement in load all age piece
## Make the plot for the mobility project

DATA_URL = "https://github.com/passaglia/japandata-sources/raw/main/population/populationdata.tar.gz"
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "populationdata/")

JAPAN_POP_CACHE = os.path.join(os.path.dirname(__file__), "japan_pop.parquet")
PREF_POP_CACHE = os.path.join(os.path.dirname(__file__), "pref_pop.parquet")
LOCAL_POP_CACHE = os.path.join(os.path.dirname(__file__), "local_pop.parquet")

JAPAN_AGE_CACHE = os.path.join(os.path.dirname(__file__), "japan_age.parquet")
PREF_AGE_CACHE = os.path.join(os.path.dirname(__file__), "pref_age.parquet")
LOCAL_AGE_CACHE = os.path.join(os.path.dirname(__file__), "local_age.parquet")


def checkfordata():
    return os.path.exists(DATA_FOLDER)


def getdata():
    if checkfordata():
        print("data already gotten")
    else:
        import urllib.request
        import tarfile

        ftpstream = urllib.request.urlopen(DATA_URL)
        rawfile = tarfile.open(fileobj=ftpstream, mode="r|gz")
        rawfile.extractall(os.path.dirname(__file__))


def load_age_data(year, datalevel="prefecture"):
    assert datalevel in ["prefecture", "local"]
    if datalevel == "prefecture":
        assert 1994 <= year <= 2022
    elif datalevel == "local":
        assert 1995 <= year <= 2022

    fileextension = ".xls"
    skiprows = 2
    if year >= 2021:
        fileextension = ".xlsx"
        skiprows = 3

    forced_coltypes = {"code6digit": str, "prefecture": str}
    cols = ["code6digit", "prefecture"]
    if datalevel == "local":
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

    ## change the column order to make sure >99 is last
    if datalevel == "prefecture":
        filelabel = str(year)[-2:] + "02"
        if year >= 2013:
            filelabel += "s"
        df = pd.read_excel(
            DATA_FOLDER + "tnen/" + filelabel + "tnen" + fileextension,
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )
    elif datalevel == "local":
        filelabel = str(year)[-2:] + "04"
        if year >= 2013:
            filelabel += "s"
        df = pd.read_excel(
            DATA_FOLDER + "snen/" + filelabel + "snen" + fileextension,
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )

    if year >= 2021:
        df = df[:-2]

    if datalevel == "local":
        df["city"].replace("\x1f", np.nan, inplace=True)
        df["city"].replace("-", np.nan, inplace=True)
        df["city"] = df["city"].str.strip()
        df["city"] = df["city"].str.replace("*", "", regex=False)
        df["city"].replace("", np.nan, inplace=True)

    df["prefecture"] = df["prefecture"].str.strip()
    df["prefecture"] = df["prefecture"].str.replace("*", "", regex=False)

    df.loc[df["prefecture"] == "合計", "code6digit"] = np.nan

    if datalevel == "local":
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
    if datalevel == "local":
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])

    if year == 2005:
        df = df.drop("total-pop-corrected", axis=1)

    ##### SELF-CONSISTENCY TESTS ###
    # This tests whether men+women = total
    grouped = df.drop(
        ["code6digit", "prefecture", "city", "gender"],
        axis=1,
        errors="ignore",
    ).groupby("code")

    def testfunc(group):
        # print(group)
        assert (group.iloc[0, :-1] == group.iloc[1, :-1] + group.iloc[2, :-1]).all()

    grouped.apply(testfunc)
    ##### SELF-CONSISTENCY TESTS ####

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

    return df


def load_pop_data(year, datalevel="prefecture", poptype="resident"):
    assert datalevel in ["prefecture", "local"]
    if datalevel == "prefecture":
        assert 1968 <= year <= 2022
    elif datalevel == "local":
        assert 1995 <= year <= 2022

    assert poptype in ["resident", "japanese", "foreigner"]
    if poptype != "resident":
        assert 2013 <= year

    fileextension = ".xls"
    skiprows = 4
    if year >= 2021:
        fileextension = ".xlsx"
        skiprows = 6

    forced_coltypes = {"code6digit": str, "prefecture": str}

    cols = ["code6digit", "prefecture"]
    if datalevel == "local":
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
        cols += ["moved-in", "born"]
        if poptype == "japanese":
            cols += ["naturalization", "other-in-other"]
        if poptype == "foreigner":
            if year == 2013:
                cols += ["other-30-47"]
            cols += ["denaturalization", "other-in-other"]
        cols += ["other-in", "total-in"]
        if 2013 <= year:
            cols += ["moved-out-domestic", "moved-out-international"]
        cols += ["moved-out", "died"]
        if poptype == "japanese":
            cols += ["denaturalization", "other-out-other"]
        if poptype == "foreigner":
            cols += ["naturalization", "other-out-other"]
        cols += ["other-out", "total-out", "in-minus-out"]
        if 1994 <= year:
            cols += ["in-minus-out-rate"]
        cols += [
            "born-minus-died",
            "born-minus-died-rate",
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
        elif poptype == "foreigner":
            filelabel = str(year)[-2:] + "09g"
        df = pd.read_excel(
            DATA_FOLDER + "tjin/" + filelabel + "tjin" + fileextension,
            skiprows=skiprows,
            header=None,
            names=cols,
            dtype=forced_coltypes,
        )
    elif datalevel == "local":
        if poptype == "resident":
            filelabel = str(year)[-2:] + "03"
            if year >= 2013:
                filelabel += "s"
        elif poptype == "japanese":
            filelabel = str(year)[-2:] + "07n"
        elif poptype == "foreigner":
            filelabel = str(year)[-2:] + "11g"
        df = pd.read_excel(
            DATA_FOLDER + "sjin/" + filelabel + "sjin" + fileextension,
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
            "born-minus-died-rate",
            "social-in-minus-social-out-rate",
        ],
        axis=1,
        errors="ignore",
    )

    if year >= 2021:
        df = df[:-1]

    if datalevel == "local":
        df["city"].replace("\x1f", np.nan, inplace=True)
        df["city"].replace("-", np.nan, inplace=True)
        df["city"] = df["city"].str.strip()

    df["prefecture"] = df["prefecture"].str.strip()
    df.loc[df["prefecture"] == "合計", "code6digit"] = np.nan

    if datalevel == "local":
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
    if datalevel == "local":
        df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])

    ##### SELF-CONSISTENCY TESTS ####
    assert (df["men"] + df["women"] == df["total-pop"]).all()
    if year >= 1980:
        assert (df["moved-in"] + df["born"] + df["other-in"] == df["total-in"]).all()
        if year != 1996 and datalevel != "local":
            assert (
                df["moved-out"] + df["died"] + df["other-out"] == df["total-out"]
            ).all()
        assert (df["total-in"] - df["total-out"] == df["in-minus-out"]).all()
        assert (df["born"] - df["died"] == df["born-minus-died"]).all()
        assert (
            df["moved-in"] + df["other-in"] - df["moved-out"] - df["other-out"]
            == df["social-in-minus-social-out"]
        ).all()
    if year >= 2013:
        assert (
            df["moved-in-domestic"] + df["moved-in-international"] == df["moved-in"]
        ).all()
        assert (
            df["moved-out-domestic"] + df["moved-out-international"] == df["moved-out"]
        ).all()
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
    ##### SELF-CONSISTENCY TESTS ####

    df["year"] = year

    return df


def generate_pop_dfs():

    poptypes = ["resident", "japanese", "foreigner"]

    complete_japan_df = pd.DataFrame()
    complete_pref_df = pd.DataFrame()
    complete_local_df = pd.DataFrame()

    for poptype in poptypes:
        print(poptype)
        if poptype == "resident":
            years = np.arange(1968, 2023)
        else:
            years = np.arange(2013, 2023)

        japan_df = pd.DataFrame()
        pref_df = pd.DataFrame()
        local_df = pd.DataFrame()
        for year in years:
            print(year)
            pref_df_year = load_pop_data(year, poptype=poptype, datalevel="prefecture")
            japan_df_year = (
                pref_df_year[pref_df_year["prefecture"] == "合計"]
                .copy()
                .drop(["prefecture", "code"], axis=1)
            )
            japan_df = pd.concat([japan_df, japan_df_year], ignore_index=True)
            pref_df_year.drop(
                pref_df_year.loc[pref_df_year["prefecture"] == "合計"].index, inplace=True
            )
            pref_df = pd.concat([pref_df, pref_df_year], ignore_index=True)

            if year >= 1995:
                local_df_year = load_pop_data(year, poptype=poptype, datalevel="local")

                ### the summary rows of the local table
                local_df_year_prefrows = (
                    local_df_year.loc[pd.isna(local_df_year["city"])]
                    .drop(["city", "code", "code6digit", "prefecture"], axis=1)
                    .reset_index(drop=True)
                )
                ### checking consistency of the japan table and the local table
                assert (
                    japan_df_year.values == local_df_year_prefrows.iloc[0].values
                ).all()
                ### checking consistency of the prefecture table and the local table
                assert (
                    pref_df_year.drop(
                        [
                            "code",
                            "prefecture",
                        ],
                        axis=1,
                    ).values
                    == local_df_year_prefrows.iloc[1:].values
                ).all()

                ## dropping the summary rows
                local_df_year = local_df_year.loc[~pd.isna(local_df_year["city"])]

                local_df = pd.concat([local_df, local_df_year], ignore_index=True)

        japan_df["poptype"] = poptype
        pref_df["poptype"] = poptype
        local_df["poptype"] = poptype

        complete_japan_df = pd.concat([complete_japan_df, japan_df], ignore_index=True)
        complete_pref_df = pd.concat([complete_pref_df, pref_df], ignore_index=True)
        complete_local_df = pd.concat([complete_local_df, local_df], ignore_index=True)

    # The years above are actually the value at the end of the previous fiscal year. Here we make it the value at the end of the current fiscal year so that population flows within a given year are now assigned correctly.

    complete_japan_df["year"] = complete_japan_df["year"] - 1
    complete_pref_df["year"] = complete_pref_df["year"] - 1
    complete_local_df["year"] = complete_local_df["year"] - 1

    return complete_japan_df, complete_pref_df, complete_local_df


def clean_age_data():

    years = np.arange(1994, 2023)
    df_japan_age_list = []
    df_pref_age_list = []
    df_local_age_list = []
    for year in years:
        print(year)
        df_pref_age = load_age_data(year, datalevel="prefecture")
        df_japan_age = (
            df_pref_age[df_pref_age["prefecture"] == "合計"]
            .copy()
            .reset_index(drop=True)
            .drop(["code", "prefecture"], axis=1)
        )
        df_japan_age_list.append(df_japan_age)
        df_pref_age = df_pref_age.set_index("prefecture").drop("合計").reset_index()
        df_pref_age_list.append(df_pref_age)

        if year >= 1995:
            df_local_age = load_age_data(year, datalevel="local")

            ### checking consistency of the summary rows of the local table with the summary table
            df_local_age_pref = df_local_age.loc[pd.isna(df_local_age["city"])]
            df_local_age_pref.reset_index(inplace=True, drop=True)
            ### checking consistency of the japan table and the local table
            assert (
                df_japan_age.drop(["gender"], axis=1, errors="ignore").values
                == df_local_age_pref.drop(
                    [
                        "prefecture",
                        "city",
                        "code",
                        "code6digit",
                        "gender",
                    ],
                    axis=1,
                    errors="ignore",
                )
                .iloc[0:3]
                .values
            ).all()
            ### checking consistency of the prefecture table and the local table
            assert (
                df_pref_age.drop(
                    ["code", "gender", "prefecture"],
                    axis=1,
                    errors="ignore",
                ).values
                == df_local_age_pref.drop(
                    [
                        "prefecture",
                        "city",
                        "code",
                        "gender",
                        "code6digit",
                    ],
                    axis=1,
                    errors="ignore",
                )
                .iloc[3:]
                .values
            ).all()

            # dropping the summary rows
            df_local_age = df_local_age.loc[~pd.isna(df_local_age["city"])]
            df_local_age_list.append(df_local_age)

    japan_age_df = pd.concat(df_japan_age_list).reset_index(drop=True)
    pref_age_df = pd.concat(df_pref_age_list).reset_index(drop=True)
    local_age_df = pd.concat(df_local_age_list).reset_index(drop=True)

    # The years above are actually the value at the end of the previous fiscal year. Here we make it the value at the end of the current fiscal year so that population flows within a given year are assigned correctly.
    japan_age_df["year"] = japan_age_df["year"] - 1
    pref_age_df["year"] = pref_age_df["year"] - 1
    local_age_df["year"] = local_age_df["year"] - 1

    return japan_age_df, pref_age_df, local_age_df


try:
    japan_pop_df = pd.read_parquet(JAPAN_POP_CACHE)
    prefecture_pop_df = pd.read_parquet(PREF_POP_CACHE)
    local_pop_df = pd.read_parquet(LOCAL_POP_CACHE)

    japan_age_df = pd.read_parquet(JAPAN_AGE_CACHE)
    prefecture_age_df = pd.read_parquet(PREF_AGE_CACHE)
    local_age_df = pd.read_parquet(LOCAL_AGE_CACHE)
except FileNotFoundError:
    if not checkfordata():
        getdata()
    japan_pop_df, prefecture_pop_df, local_pop_df = generate_pop_dfs()
    japan_pop_df.to_parquet(JAPAN_POP_CACHE)
    prefecture_pop_df.to_parquet(PREF_POP_CACHE)
    local_pop_df.to_parquet(LOCAL_POP_CACHE)

    japan_age_df, prefecture_age_df, local_age_df = clean_age_data()
    japan_age_df.to_parquet(JAPAN_AGE_CACHE)
    prefecture_age_df.to_parquet(PREF_AGE_CACHE)
    local_age_df.to_parquet(LOCAL_AGE_CACHE)

# Be careful -- local pop df contains duplicates (i.e. a municipality and its subcomponents)
