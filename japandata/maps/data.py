"""
maps/data.py

Module which loads, caches, and provides access to maps of japan

Author: Sam Passaglia
"""

import geopandas as gpd
import pandas as pd
import os
import numpy as np
import urllib.request
import shutil
from urllib.error import HTTPError

CACHE_FOLDER = os.path.join(os.path.dirname(__file__), "cache/")

ORIGINAL_URL = "https://geoshape.ex.nii.ac.jp/city/topojson/"
MIRROR_URL = "https://github.com/passaglia/japandata-sources/raw/main/map/"
BASE_URL = MIRROR_URL

available_dates = np.array(
    [
        "1920-01-01",
        "1950-10-01",
        "1955-10-01",
        "1960-10-01",
        "1965-10-01",
        "1970-10-01",
        "1975-10-01",
        "1980-10-01",
        "1985-10-01",
        "1995-10-01",
        "2000-10-01",
        "2005-01-01",
        "2006-01-01",
        "2007-04-01",
        "2007-10-01",
        "2009-03-20",
        "2010-03-29",
        "2011-03-31",
        "2012-04-01",
        "2013-04-01",
        "2014-04-01",
        "2015-01-01",
        "2016-01-01",
        "2017-01-01",
        "2018-01-01",
        "2019-01-01",
        "2020-01-01",
        "2021-01-01",
    ],
    dtype="datetime64",
)

extension_dict = {
    "local_dc": ".topojson",
    "local": ".topojson",
    "prefecture": ".topojson",
    "japan": ".geojson",
}
level_filename_dict = {
    "local_dc": "jp_city_dc",
    "local": "jp_city",
    "prefecture": "jp_pref",
    "japan": "jp",
}
quality_suffix_dict = {"coarse": "c", "low": "l", "medium": "i", "high": "h"}


def load_map(date=2022, level="local_dc", quality="coarse"):
    try:
        date = np.datetime64(date)
    except ValueError:
        date = np.datetime64(str(date) + "-12-31")
    assert date >= np.min(available_dates)

    assert quality in ["stylized", "coarse", "low", "medium", "high"]

    assert level in ["japan", "prefecture", "local", "local_dc"]

    if level == "japan" and quality not in ["stylized", "coarse"]:
        print("japan only available at stylized or coarse level")
        assert quality in ["stylized", "coarse"]

    if quality == "stylized":
        # For stylized charts we need to remove islands and such at the local level
        # Then rejoin to get the pref/japan levels
        if level == "prefecture":
            return join_localities(
                remove_islands(load_map(date, "local_dc", "stylized"))
            )
        elif level == "japan":
            return join_prefectures(load_map(date, "prefecture", "stylized"))
        else:
            # If want a local stylized chart
            if date > np.datetime64(str(2017) + "-12-31"):
                # either load a pre-stylized one (if year is modern enough)
                needed_file = level_filename_dict[level] + ".stylized.json"
            else:
                # construct one ourselves from the coarse charts
                return stylize(load_map(date, level, "coarse"))
    else:
        # All non-stylized charts we just download from server
        needed_date = str(
            np.max(
                available_dates[np.where(date - available_dates >= np.timedelta64(0))]
            )
        )
        needed_file = (
            needed_date.replace("-", "")
            + "/"
            + level_filename_dict[level]
            + "."
            + quality_suffix_dict[quality]
            + extension_dict[level]
        )

    url = BASE_URL + needed_file

    if os.path.exists(CACHE_FOLDER + needed_file):
        map_df = gpd.read_file(CACHE_FOLDER + needed_file)
    else:
        print("cache not found. fetching from server")
        try:
            response = urllib.request.urlopen(url)
            data = response.read()  # a `bytes` object
            text = data.decode("utf-8")
            os.makedirs(os.path.dirname(CACHE_FOLDER + needed_file), exist_ok=True)
            with open(CACHE_FOLDER + needed_file, "w") as f:
                f.write(text)
        except HTTPError:
            print("file not found on server")
            if level == "japan":
                print("trying to generate japan file from prefectures")
                from shapely.ops import unary_union
                from shapely.validation import make_valid

                pref_df = load_map(
                    date=needed_date, level="prefecture", quality=quality
                )
                polygonlist = []
                for i in range(len(pref_df)):
                    print(i)
                    polygonlist.append(make_valid(pref_df.geometry[i]))
                map_df = gpd.GeoDataFrame(
                    geometry=[unary_union(polygonlist)], crs=pref_df.crs
                )
                map_df.to_file(CACHE_FOLDER + needed_file, driver="GeoJSON")
            else:
                return Exception("Map was not found")
        map_df = gpd.read_file(CACHE_FOLDER + needed_file)

    map_df.crs = "EPSG:6668"

    # column headers are explained at https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N03-v2_2.html
    map_df.rename(
        columns={
            "N03_001": "prefecture",
            "N03_002": "bureau",
            "N03_003": "county",
            "N03_004": "city",
            "N03_005": "founding_date",
            "N03_006": "extinction_date",
            "N03_007": "code",
            "type": "special",
        },
        inplace=True,
        errors="ignore",
    )

    if level == "prefecture":
        map_df.drop(
            columns=[
                "id",
                "bureau",
                "founding_date",
                "extinction_date",
                "city",
                "county",
            ],
            errors="ignore",
            inplace=True,
        )

    if level == "local_dc":
        map_df.loc[map_df["special"] == "designated-city", "city"] = map_df.loc[
            map_df["special"] == "designated-city", "county"
        ]

    if level == "local_dc" or "local":
        map_df.drop(columns=["id"], errors="ignore", inplace=True)

    map_df["prefecture"] = map_df["prefecture"].str.replace("沖繩", "沖縄")
    map_df["id"] = map_df["prefecture"].str.replace("沖繩", "沖縄")

    return map_df


def remove_islands(local_df):
    island_codes_hokkaido = [
        "01518",
        "01519",
        "01517",
        "01367",
        "01695",
        "01696",
        "01697",
        "01698",
        "01699",
        "01700",
    ]
    island_codes_wakkayama = []  # ['30427']
    island_codes_ooita = ["44322"]
    island_codes_yamaguchi = ["35305"]
    island_codes_tottori = ["32528", "32525", "32526", "32527"]
    island_codes_tokyo = [
        "13421",
        "13361",
        "13362",
        "13363",
        "13364",
        "13381",
        "13382",
        "13401",
        "13402",
    ]

    island_codes = (
        island_codes_hokkaido
        + island_codes_wakkayama
        + island_codes_ooita
        + island_codes_yamaguchi
        + island_codes_tottori
        + island_codes_tokyo
    )

    return local_df.drop(
        local_df.loc[local_df["code"].isin(island_codes)].index
    ).reset_index(drop=True)


def join_localities(local_df):
    from shapely.ops import unary_union
    from shapely.validation import make_valid

    local_df = local_df.drop(["city", "bureau", "county", "code"], axis=1)
    prefs = []
    polygons = []
    for prefecture in local_df["prefecture"].unique():
        polygonlist = []
        for geometry in local_df.loc[local_df["prefecture"] == prefecture, "geometry"]:
            polygonlist.append(make_valid(geometry))
        polygons.append(unary_union(polygonlist))
        prefs.append(prefecture)

    pref_df = gpd.GeoDataFrame(
        prefs, columns=["prefecture"], geometry=polygons, crs=local_df.crs
    )
    return pref_df


def join_prefectures(pref_df):
    from shapely.ops import unary_union
    from shapely.validation import make_valid

    polygonlist = []
    for i in range(len(pref_df)):
        polygonlist.append(make_valid(pref_df.geometry[i]))
    japan_df = gpd.GeoDataFrame(geometry=[unary_union(polygonlist)], crs=pref_df.crs)
    return japan_df


def stylize(local_df):
    import shapely
    import topojson as tp

    # meters coords
    local_df = remove_islands(local_df.to_crs("EPSG:30166"))
    local_df = local_df.loc[~local_df["geometry"].is_empty]
    local_df = local_df.loc[~(local_df["code"].isnull())]
    topojson = tp.Topology(local_df, prequantize=False)
    local_df = topojson.toposimplify(
        1000,  ## this is in meters
        prevent_oversimplify=False,
    ).to_gdf()
    local_df["geometry"] = [
        shapely.geometry.MultiPolygon(
            [P for P in geometry if P.area > 1000 * 1000]
        )  ## meters * meters
        for geometry in local_df["geometry"]
    ]
    # lat long coords
    local_df = local_df.to_crs("EPSG:6668")

    return local_df


def get_dates():
    return available_dates


def clear_cache():
    shutil.rmtree(CACHE_FOLDER)


def generate_cache(levels=["local_dc"], qualities=["coarse"]):
    for level in levels:
        print(level)
        for quality in qualities:
            print(quality)
            for date in available_dates:
                print(date)
                load_map(date, level, quality)


# Helper function to add a df to a map
def add_df_to_map(
    df,
    date,
    level,
    quality="coarse",
):

    map_df = load_map(date, level=level, quality=quality)
    map_df = map_df.loc[~map_df["geometry"].is_empty]
    if level == "prefecture":
        merge_tokens = ["prefecture"]
    else:
        map_df = map_df.loc[~(map_df["code"].isnull())]
        map_df = map_df.drop_duplicates(subset="code")
        merge_tokens = ["prefecture", "code"]
    merged_df = pd.merge(
        map_df,
        df,
        on=merge_tokens,
        how="left",
        suffixes=["", "_df"],
        validate="one_to_one",
        indicator=True,
    )
    assert len(merged_df) == len(map_df)

    print(len(merged_df.loc[(merged_df["_merge"] == "left_only")]), "failures")

    merged_df = gpd.GeoDataFrame(merged_df)
    return merged_df


##
## Mapshaper commands to get a nice filtered japan map
# -filter "!['01518', '01519','01517', '01367', '01695', '01696', '01697', '01698','01699', '01700','44322','35305','32528','32525','32526','32527','13421','13361','13362','13363','13364','13381','13382','13401','13402'].includes(N03_007)"
# -clean
# -explode
# -filter-islands min-area 600km2
# -dissolve2
# -clean
# -clean
# -explode
# -filter-islands min-area 200km2
# -dissolve2
# -clean

## Mapshaper commands to get a nice filtered local map
# -filter "!['01518', '01519','01517', '01367', '01695', '01696', '01697', '01698','01699', '01700','44322','35305','32528','32525','32526','32527','13421','13361','13362','13363','13364','13381','13382','13401','13402'].includes(N03_007)"
# -clean
# -filter-islands min-area 600km2
# -clean


# ##################################
# ### Plot all maps for debug ######
# ##################################
# rotation_origin = None
# for year in range(1995, 2020):
#     # for year in range(1994, 1995):
#     print(year)
#     print("loading map")
#     df = load_map(year, level="local_dc", quality="coarse")
#     df = df.loc[~df["geometry"].is_empty]
#     df = remove_islands(df)

#     print("moving okinawa")
#     df.loc[df["prefecture"] == "沖縄県", "geometry"] = df.loc[
#         df["prefecture"] == "沖縄県", "geometry"
#     ].affine_transform([1, 0, 0, 1, 6.5, 13])

#     print("rotating")
#     rotation_angle = -17
#     if rotation_origin is None:
#         rotation_origin = df[df.is_valid].unary_union.centroid
#     df["geometry"] = df["geometry"].rotate(rotation_angle, origin=rotation_origin)

#     print("loading as topojson")
#     df = df.to_crs("EPSG:30166")
#     topojson = tp.Topology(df, prequantize=False)
#     print("simplifying")
#     df = topojson.toposimplify(
#         2000,
#         prevent_oversimplify=False,
#     ).to_gdf()
#     print("removing small things")
#     df = df.to_crs("EPSG:30166")
#     df["geometry"] = [
#         shapely.geometry.MultiPolygon([P for P in geometry if P.area > 1000 * 1000])
#         for geometry in df["geometry"]
#     ]

#     print("plotting")
#     df = df.to_crs("EPSG:30166")
#     bp = BasePlot(figsize=(6, 6), fontsize=12)
#     fig, ax = bp.handlers()
#     ax = df.plot(
#         column="code",
#         ax=ax,
#         legend=False,
#         lw=0.05,
#         edgecolor="none"
#         # edgecolor="#4341417c",
#     )
#     plt.axis("off")
#     ax.set_xlim([-0.75 * 10**6, 0.98 * 10**6])
#     ax.set_ylim([-0.28 * 10**6, 0.95 * 10**6])

#     print("saving")
#     fig.savefig("local/" + str(year) + ".pdf")
#     plt.close("all")
