"""
furusatonouzei/data.py

Module which processes and provides access to data about the furusato nouzei tax program.

Author: Sam Passaglia
"""

import pandas as pd
import numpy as np
import os

DATA_URL = "https://github.com/passaglia/japandata-sources/raw/main/furusatonouzei/furusatonouzeidata.tar.gz"

FN_DATA_FOLDER = os.path.join(os.path.dirname(__file__), "furusatonouzeidata/")
DONATIONS_ROUGH_FILE = os.path.join(FN_DATA_FOLDER, "donations-rough/total_gain.xlsx")
DONATIONS_FOLDER = os.path.join(FN_DATA_FOLDER, "donations/")
DEDUCTIONS_FOLDER = os.path.join(FN_DATA_FOLDER, "deductions/")

CACHED_FILE = os.path.join(os.path.dirname(__file__), "cleandata.parquet")
ROUGH_CACHED_FILE = os.path.join(os.path.dirname(__file__), "roughdata.parquet")


def checkfordata():
    return os.path.exists(FN_DATA_FOLDER)


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


def load_donations_rough():

    DONATIONS_ROUGH_FILE = os.path.join(
        FN_DATA_FOLDER, "donations-rough/total_gain_backup.xlsx"
    )
    years = ["H" + str(i) for i in range(20, 31)] + ["R1", "R2"]
    western_years = list(range(2008, 2021))

    DONATIONS_ROUGH_FILE = os.path.join(
        FN_DATA_FOLDER, "donations-rough/total_gain.xlsx"
    )
    years = ["H" + str(i) for i in range(20, 31)] + ["R1", "R2", "R3"]
    western_years = list(range(2008, 2022))

    colnames = ["prefecture", "city"]
    for year in western_years:
        colnames.append(
            str(year) + "-donations"
        )  # units of this column is thousands of yen (for now)
        colnames.append(str(year) + "-donations-count")

    df = pd.read_excel(DONATIONS_ROUGH_FILE, header=3, names=colnames)

    for i in range(len(df["prefecture"])):
        if df["prefecture"][i] == "市町村合計":
            df.at[i, "prefecture"] = df["prefecture"][i - 1]
            df.at[i, "city"] = "prefecture_cities_total"
        elif df["prefecture"][i] == "合計":
            df.at[i, "prefecture"] = df["prefecture"][i - 1]
            df.at[i, "city"] = "prefecture_all_total"
        elif df["prefecture"][i] == "全国合計":
            df.at[i, "prefecture"] = "japan"
            df.at[i, "city"] = "total"
        if pd.isna(df["city"][i]):
            df.at[i, "city"] = "prefecture"

    for year in western_years:
        df[str(year) + "-donations"] = (
            df[str(year) + "-donations"] * 1000
        )  # units of this column is now yen

    prefecture_list = df["prefecture"].unique().tolist()
    prefecture_list.remove("japan")

    for i in range(len(prefecture_list)):
        prefecture = prefecture_list[i]
        rows = df.loc[df["prefecture"] == prefecture]
        data = rows.iloc[1:-2, 2:]
        computed_cities_subtotal = data.sum(axis=0)
        written_cities_subtotal = rows.iloc[-2, 2:]
        assert np.abs((computed_cities_subtotal - written_cities_subtotal).sum()) < 5000
        computed_subtotal = computed_cities_subtotal + rows.iloc[0, 2:]
        written_subtotal = rows.iloc[-1, 2:]
        assert np.abs((computed_subtotal - written_subtotal).sum()) < 5000
        if i == 0:
            total = computed_subtotal
        else:
            total += computed_subtotal

    written_total = df.loc[df["prefecture"] == "japan"].iloc[0, 2:]
    assert (np.abs(written_total - total) < 5000).all()

    df = df.loc[
        (df["city"] != "total")
        & (df["city"] != "prefecture_all_total")
        & (df["city"] != "prefecture_cities_total")
    ]

    df["prefecturecity"] = df["prefecture"] + df["city"]

    df.reset_index(drop=True, inplace=True)

    return df


def load_donations_by_year(year, correct_errors=True):

    cols = {
        "code6digit": str,
        "prefecture": str,
        "city": str,
        "donations-count": np.int64,
        "donations": np.int64,
        "donations-from-outside-count": np.int64,
        "donations-from-outside": np.int64,
        "donations-disaster-count": np.int64,
        "donations-disaster": np.int64,
        "product-cost": np.int64,
        "shipping-cost": np.int64,
        "total-cost": np.int64,
    }

    forced_coltypes = {"code6digit": str, "prefecture": str}

    if year == "R3":
        columnindices = [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 17]
        skiprows = 12
        ncols = 97
    if year == "R2":
        columnindices = [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 17]
        skiprows = 13
        ncols = 96
    if year == "R1":
        columnindices = [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 17]
        skiprows = 16
        ncols = 114
    if year == "H30":
        columnindices = [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 17]
        skiprows = 5
        ncols = 114
    if year == "H29":
        columnindices = [0, 1, 2, 3, 4, 6, 7, 12, 13, 24, 25, 30]
        skiprows = 5
        ncols = 118
    if year == "H28":
        columnindices = [0, 1, 2, 3, 4, 6, 7, 12, 13, 24, 25, 30]
        skiprows = 6
        ncols = 127

    df = pd.read_excel(
        DONATIONS_FOLDER + year + "_gain.xlsx",
        skiprows=skiprows,
        header=None,
        usecols=columnindices,
        names=cols.keys(),
        dtype=forced_coltypes,
    )
    df.loc[(df.city == 0) | pd.isna(df.city) | (df.city == "-"), "city"] = "prefecture"

    if (year == "H29") or (year == "H28"):
        df.loc[(df.city == "篠山市"), "city"] = "丹波篠山市"
        df.loc[(df.city == "那珂川町") & (df.prefecture == "福岡県"), "city"] = "那珂川市"

    df.loc[(df.prefecture == "鹿児島"), "prefecture"] = "鹿児島県"
    df.loc[(df.prefecture == "岡山"), "prefecture"] = "岡山県"

    df["prefecturecity"] = df["prefecture"] + df["city"]
    df["donations-from-outside"] = (
        pd.to_numeric(df["donations-from-outside"], errors="coerce")
        .fillna(0)
        .astype("int")
    )
    df["donations-from-outside-count"] = (
        pd.to_numeric(df["donations-from-outside-count"], errors="coerce")
        .fillna(0)
        .astype("int")
    )
    df["product-cost"] = (
        pd.to_numeric(df["product-cost"], errors="coerce").fillna(0).astype("int")
    )
    df["shipping-cost"] = (
        pd.to_numeric(df["shipping-cost"], errors="coerce").fillna(0).astype("int")
    )
    df["total-cost"] = (
        pd.to_numeric(df["total-cost"], errors="coerce").fillna(0).astype("int")
    )
    df["donations-disaster"] = (
        pd.to_numeric(df["donations-disaster"], errors="coerce").fillna(0).astype("int")
    )
    df["donations-disaster-count"] = (
        pd.to_numeric(df["donations-disaster-count"], errors="coerce")
        .fillna(0)
        .astype("int")
    )

    df = df.astype(cols)

    if correct_errors:
        if year == "H28":
            ## Fat finger errors here led to outrageously large donations-from-outside values
            df.loc[df["prefecturecity"] == "北海道芽室町", "donations-from-outside"] = df.loc[
                df["prefecturecity"] == "北海道芽室町", "donations"
            ]
            df.loc[df["prefecturecity"] == "山形県朝日町", "donations-from-outside"] = df.loc[
                df["prefecturecity"] == "山形県朝日町", "donations"
            ]
            df.loc[
                df["prefecturecity"] == "東京都神津島村", "donations-from-outside"
            ] = df.loc[df["prefecturecity"] == "東京都神津島村", "donations"]
            df.loc[
                df["prefecturecity"] == "山梨県北杜市", "donations-from-outside"
            ] = 13227000
            ## Here it looks like within and without got swapped
            df.loc[
                df["prefecturecity"] == "栃木県那珂川町",
                [
                    "donations-count",
                    "donations",
                    "donations-from-outside-count",
                    "donations-from-outside",
                ],
            ] = df.loc[
                df["prefecturecity"] == "栃木県那珂川町",
                [
                    "donations-from-outside-count",
                    "donations-from-outside",
                    "donations-count",
                    "donations",
                ],
            ].values
            ## Fat finger error leads to outrageously small gain value
            df.loc[df["prefecturecity"] == "埼玉県上里町", "donations"] = (
                df.loc[df["prefecturecity"] == "埼玉県上里町", "donations"] * 10
            )  # 1650000
            df.loc[df["prefecturecity"] == "静岡県御殿場市", "donations"] = (
                df.loc[df["prefecturecity"] == "静岡県御殿場市", "donations"] * 10
            )
            ## Minor typo
            df.loc[df["prefecturecity"] == "山梨県prefecture", "donations"] = 24151001
            df.loc[df["prefecturecity"] == "鹿児島県薩摩川内市", "donations"] = df.loc[
                df["prefecturecity"] == "鹿児島県薩摩川内市", "donations-from-outside"
            ]
        if year == "H29":
            pass
        if year == "H30":
            ## Fat finger errors here led to major error in donations-from-outside
            df.loc[df["prefecturecity"] == "宮崎県諸塚村", "donations-from-outside"] = df.loc[
                df["prefecturecity"] == "宮崎県諸塚村", "donations"
            ]
            ## Fat finger errors here led to minor errors in donations-from-outside
            df.loc[df["prefecturecity"] == "北海道羅臼町", "donations-from-outside"] = df.loc[
                df["prefecturecity"] == "北海道羅臼町", "donations"
            ]
            df.loc[df["prefecturecity"] == "静岡県湖西市", "donations-from-outside"] = df.loc[
                df["prefecturecity"] == "静岡県湖西市", "donations"
            ]
            df.loc[df["prefecturecity"] == "熊本県八代市", "donations-from-outside"] = df.loc[
                df["prefecturecity"] == "熊本県八代市", "donations"
            ]
            df.loc[
                df["prefecturecity"] == "鹿児島県湧水町", "donations-from-outside"
            ] = df.loc[df["prefecturecity"] == "鹿児島県湧水町", "donations"]
        if year == "R1":
            ## Minor issue
            df.loc[df["prefecturecity"] == "福井県美浜町", "donations"] = df.loc[
                df["prefecturecity"] == "福井県美浜町", "donations-from-outside"
            ]  # 1650000

    df["net-gain"] = df["donations"] - df["total-cost"]

    df["code"] = df["code6digit"].apply(lambda s: s if pd.isna(s) else s[:-1])
    df.drop(["code6digit"], inplace=True, axis=1)

    if correct_errors:
        assert len(df.loc[(df["donations-from-outside"] - 1 > df["donations"])]) == 0
    # problematic_rows = df.loc[df["donations-from-outside"]-1 > df["donations"]].index
    # print(df.loc[problematic_rows, ["prefecturecity", "donations", "donations-count", "donations-from-outside", "donations-from-outside-count"]])
    # print(df.loc[problematic_rows, "donations"]/ df.loc[problematic_rows, "donations-count"])

    # print(df.loc[(df["donations-from-outside"] <  df["donations"]/6) & (df["donations-from-outside"]>0)])

    # print(df.loc[(df["donations"]/df["donations-count"]) > 10**6])
    # print(df.loc[(df["donations"]/df["total-cost"]) > 100])

    ## Flagging unfixable errors

    df["flag"] = False
    df.loc[(df["product-cost"] + df["shipping-cost"]) > df["donations"], "flag"] = True
    # print(df.loc[(df["product-cost"] + df["shipping-cost"]) > df["donations"]])

    # print(df['donations-from-outside'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index]/df['donations-count'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index])
    # print(df['donations'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index]/df['donations-count'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index])

    # plt.plot(np.sort(df["donations"] / df["donations-count"]))
    # plt.yscale('log')
    # plt.show()

    return df


def load_deductions_by_year(year):
    cols = {
        "prefecture": object,
        "city": object,
        "city-reported-people": np.int64,
        "city-reported-donations": np.int64,
        "deductions": np.int64,
        "pref-reported-people": np.int64,
        "pref-reported-donations": np.int64,
        "pref-tax-deductions": np.int64,
    }

    if year == "R4":
        columnindices = [0, 1, 53, 54, 55, 56, 57, 58]
        skiprows = 18
        sheetnumber = 0
        ncols = 59
    if year == "R3":
        columnindices = [0, 1, 53, 54, 55, 56, 57, 58]
        skiprows = 18
        sheetnumber = 0
        ncols = 59
    if year == "R2":
        columnindices = [0, 1, 53, 54, 55, 56, 57, 58]
        skiprows = 18
        sheetnumber = 1
        ncols = 59
    if year == "R1":
        columnindices = [0, 1, 53, 54, 55, 56, 57, 58]
        skiprows = 18
        sheetnumber = 0
        ncols = 59
    if year == "H30":
        columnindices = [0, 1, 52, 53, 54, 55, 56, 57]
        skiprows = 19
        sheetnumber = 0
        ncols = 58
    if year == "H29":
        columnindices = [0, 1, 53, 54, 55, 56, 57, 58]
        skiprows = 17
        sheetnumber = 0
        ncols = 59
    if year == "H28":
        columnindices = [0, 1, 53, 54, 55, 56, 57, 58]
        skiprows = 15
        sheetnumber = 0
        ncols = 59

    df = pd.read_excel(
        DEDUCTIONS_FOLDER + year + "_loss.xlsx",
        skiprows=skiprows,
        sheet_name=sheetnumber,
        header=None,
        usecols=columnindices,
        names=cols.keys(),
    )

    for i in range(len(df["prefecture"])):
        if df["prefecture"][i][-2:] == "集計" or df["prefecture"][i] == "合計":
            df.at[i, "prefecture"] = df["prefecture"][i - 1]
            df.at[i, "city"] = "prefecture_cities_total"
        if df["prefecture"][i] == "総計" or df["prefecture"][i] == "全国合計":
            df.at[i, "prefecture"] = "japan"
            df.at[i, "city"] = "total"

    df.loc[(df.prefecture == "岡山"), "prefecture"] = "岡山県"
    df.loc[(df.prefecture == "沖縄"), "prefecture"] = "沖縄県"
    df.loc[(df.prefecture == "青森"), "prefecture"] = "青森県"
    df.loc[(df.city == "篠山市"), "city"] = "丹波篠山市"
    df.loc[(df.city == "那珂川町") & (df.prefecture == "福岡県"), "city"] = "那珂川市"
    df.loc[(df.prefecture == "鹿児島"), "prefecture"] = "鹿児島県"
    df.loc[(df.city == "宝塚市"), "city"] = "宝塚市"
    df.loc[(df.city == "富谷町"), "city"] = "富谷市"

    prefecture_list = df["prefecture"].unique().tolist()
    prefecture_list.remove("japan")

    for i in range(len(prefecture_list)):
        prefecture = prefecture_list[i]
        rows = df.loc[df["prefecture"] == prefecture]
        data = rows.iloc[0:-1, 2:]
        computed_subtotal = data.sum(axis=0)
        written_subtotal = rows.iloc[-1, 2:]
        assert np.abs((computed_subtotal - written_subtotal).sum()) < 1
        if i == 0:
            total = computed_subtotal
        else:
            total += computed_subtotal

    written_total = df.loc[df["prefecture"] == "japan"].iloc[0, 2:]
    assert (np.abs(written_total - total) < 100).all()

    df = df.astype(cols)

    df["prefecturecity"] = df["prefecture"] + df["city"]
    df["reported-people"] = df[["city-reported-people", "pref-reported-people"]].max(
        axis=1
    )
    df["reported-donations"] = df[
        ["city-reported-donations", "pref-reported-donations"]
    ].max(axis=1)

    df = df.loc[(df["city"] != "total") & (df["city"] != "prefecture_cities_total")]
    df.reset_index(drop=True, inplace=True)

    df["prefecturecity"] = df["prefecture"] + df["city"]

    return df


def combine_loss_gain(df_loss, df_gain):
    preftaxdeduction_df = df_loss.groupby("prefecture")[
        "pref-tax-deductions"
    ].sum()  # .reset_index()

    df = pd.merge(
        df_gain,
        df_loss,
        how="left",
        on=["prefecture", "city", "prefecturecity"],
        indicator=True,
    )
    assert len(df.loc[df["_merge"] == "left_only"]) == 47

    for i in range(len(df)):
        if pd.isna(df.loc[i, "pref-tax-deductions"]):
            df.loc[i, "deductions"] = preftaxdeduction_df[df.loc[i, "prefecture"]]

    assert (
        df.loc[df["prefecturecity"] == "北海道prefecture", "deductions"].values[0]
        == preftaxdeduction_df["北海道"]
    )

    df = df.drop("_merge", axis=1)

    df["netgainminusdeductions"] = df["net-gain"] - df["deductions"]
    # df = df_gain.copy()

    # new_columns = []
    # for column in df_loss.columns:
    #     if column not in df_gain.columns:
    #         new_columns.append(column)
    #         df[column] = np.nan

    # df['netgainminusdeductions'] = np.nan

    # loss_index = 0
    # for i in range(len(df_gain)):

    #     if df_gain['city'][i]!='prefecture':
    #         try:
    #             loss_index = df_loss[df_loss['prefecturecity'] == df_gain['prefecturecity'][i]].index[0]
    #             for column in new_columns:
    #                 df.loc[i, column] = df_loss[column][loss_index]
    #                 df.loc[i,'netgainminusdeductions']= df_gain['net-gain'][i]-df_loss['deductions'][loss_index]
    #         except IndexError():
    #             print("couldn't find city")

    # df = df.set_index('prefecturecity')

    return df


def clean_data(correct_errors=True):

    print("loading rough donations table")
    df_rough = load_donations_rough()

    years = np.array([int(i) for i in [2016, 2017, 2018, 2019, 2020, 2021]])
    year_labels = ["H28", "H29", "H30", "R1", "R2", "R3"]
    corresponding_loss_labels = ["H29", "H30", "R1", "R2", "R3", "R4"]

    df = pd.DataFrame()
    for i, year in enumerate(year_labels):
        print("loading gain ", year)
        df_gain = load_donations_by_year(year, correct_errors=correct_errors)
        print("loading loss ", corresponding_loss_labels[i])
        df_loss = load_deductions_by_year(corresponding_loss_labels[i])
        print("running consistency checks ", year)
        assert (df_gain["prefecturecity"] == df_rough["prefecturecity"]).all()
        if not correct_errors:
            assert (
                np.abs(df_gain["donations"] - df_rough[str(years[i]) + "-donations"])
                < 1000
            ).all()

        print("merging loss and gain dfs")
        year_df = combine_loss_gain(df_loss, df_gain)
        year_df["year"] = years[i]
        df = pd.concat([df, year_df])
        # year_df_list.append(year_df)

    ## Now we also massage the rough array into an easier to handle form
    western_years = list(range(2008, 2022))
    df_rough["code"] = df_gain[
        "code"
    ]  ## This assumes that the municipalities are listed in the same order in the two files

    df_donations = df_rough.drop(
        [str(year) + "-donations-count" for year in western_years], axis=1
    )
    df_donations.columns = df_donations.columns.str.replace("-donations", "")
    df_donations = pd.melt(
        df_donations,
        id_vars=["prefecture", "city", "prefecturecity", "code"],
        value_vars=[str(year) for year in western_years],
        var_name="year",
        value_name="donations",
    )

    df_count = df_rough.drop(
        [str(year) + "-donations" for year in western_years], axis=1
    )
    df_count.columns = df_count.columns.str.replace("-donations-count", "")
    df_count = pd.melt(
        df_count,
        id_vars=["prefecture", "city", "prefecturecity", "code"],
        value_vars=[str(year) for year in western_years],
        var_name="year",
        value_name="donations-count",
    )

    df_rough_melted = df_donations.merge(df_count)
    df_rough_melted["year"] = pd.to_numeric(df_rough_melted["year"]).astype("int")

    return df, df_rough_melted


try:
    furusato_df = pd.read_parquet(CACHED_FILE)
    furusato_rough_df = pd.read_parquet(ROUGH_CACHED_FILE)
except FileNotFoundError:
    if not checkfordata():
        getdata()
    furusato_df, furusato_rough_df = clean_data()
    furusato_df.to_parquet(CACHED_FILE)
    furusato_rough_df.to_parquet(ROUGH_CACHED_FILE)
