"""
maps/maps.py

Module which fetches topojson maps of japan. Original data from https://geoshape.ex.nii.ac.jp/city/choropleth/

Author: Sam Passaglia
"""

import os
import urllib.request
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from japandata.utils import load_dict

CACHE_FOLDER = Path(os.path.dirname(__file__), "cache/")

###########################################
####### File fetching and caching  ########
###########################################


def fetch_file(fname):
    """Fetches and caches file

    Args:
        fname (Path): name of file to fetch

    Returns:
        Path: cached filepath.
    """

    cached = Path(CACHE_FOLDER, fname)
    if not cached.exists():
        from japandata.download import DOWNLOAD_INFO, download_progress

        url = DOWNLOAD_INFO["maps"]["latest"]["url"] + fname
        print(url)
        download_progress(url, cached)
    return cached


def fetch_manifest():
    """Fetches and caches the map manifest file.

    Returns:
        Path: cached manifest filepath
    """

    return fetch_file("manifest.json")


def fetch_map(map_date, scale, quality):
    """Fetches and caches maps of japan at a given scale and quality.
    Does NOT check if map exists in manifest.

    Args:
        map_date (datetime64 or str): exact date to fetch
        scale (str): scale of map to fetch
        quality (str): quality of map to fetch

    Returns:
        Path: cached map filepath
    """

    if quality == "s":
        extension = ".json"
    else:
        extension_dict = {
            "jp_city_dc": ".topojson",
            "jp_city": ".topojson",
            "jp_pref": ".topojson",
            "jp": ".geojson",
        }
        extension = extension_dict[scale]

    fname = map_date.replace("-", "") + "/" + scale + "." + quality + extension

    return fetch_file(fname)


##########################################
######## Map Cleaning Functions ##########
##########################################

# TODO: add docstrings to these functions


def remove_islands(city_df):
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

    return city_df[~city_df["code"].isin(island_codes)].reset_index(drop=True)


def join_cities(city_df):
    from shapely.ops import unary_union
    from shapely.validation import make_valid

    city_df = city_df.drop(["city", "bureau", "county", "code"], axis=1)
    prefs = []
    polygons = []
    for prefecture in city_df["jp_pref"].unique():
        polygonlist = []
        for geometry in city_df.loc[city_df["jp_pref"] == prefecture, "geometry"]:
            polygonlist.append(make_valid(geometry))
        polygons.append(unary_union(polygonlist))
        prefs.append(prefecture)

    pref_df = gpd.GeoDataFrame(
        prefs, columns=["jp_pref"], geometry=polygons, crs=city_df.crs
    )
    return pref_df


def join_prefectures(pref_df):
    from shapely.ops import unary_union
    from shapely.validation import make_valid

    polygons = []
    for i in range(len(pref_df)):
        polygons.append(make_valid(pref_df.geometry[i]))
    japan_df = gpd.GeoDataFrame(geometry=[unary_union(polygons)], crs=pref_df.crs)
    return japan_df


def stylize(city_df):
    import shapely
    import topojson as tp

    # meters coords
    city_df = remove_islands(city_df.to_crs("EPSG:30166"))
    city_df = city_df.loc[~city_df["geometry"].is_empty]
    city_df = city_df.loc[~(city_df["code"].isnull())]
    topojson = tp.Topology(city_df, prequantize=False)
    city_df = topojson.toposimplify(
        1000,  ## this is in meters
        prevent_oversimplify=False,
    ).to_gdf()
    city_df["geometry"] = [
        shapely.geometry.MultiPolygon(
            [P for P in geometry.geoms if P.area > 1000 * 1000]
        )  # meters * meters
        for geometry in city_df["geometry"]
    ]
    # lat long coords
    city_df = city_df.to_crs("EPSG:6668")

    return city_df


##########################################
######## Map Loading Functions ##########
##########################################

# get the manifest file listing available maps
manifest_file = fetch_manifest()
AVAILABLE_MAPS = load_dict(manifest_file)
AVAILABLE_DATES = [np.datetime64(date) for date in list(AVAILABLE_MAPS.keys())]


def load_map(date=2022, scale="jp_city_dc", quality="coarse"):
    """Load a map of japan at a given scale and quality.
    Args:
        map_date (datetime64 or str): approximate date of desired map
        scale (str): scale of map to fetch
        quality (str): quality of map to fetch

    Returns:
        geopandas dataframe: topojson map
    """

    quality_arg_dict = {
        "stylized": "s",
        "coarse": "c",
        "low": "l",
        "medium": "i",
        "high": "h",
    }
    try:
        quality = quality_arg_dict[quality]
    except KeyError as e:
        pass

    # determine the map date to use
    try:
        date = np.datetime64(date)
    except ValueError:
        date = np.datetime64(str(date) + "-12-31")

    try:
        map_date = str(
            np.array(AVAILABLE_DATES)[np.where(date >= np.array(AVAILABLE_DATES))][-1]
        )
    except IndexError as e:
        raise Exception(f"date must be >= than {str(np.min(AVAILABLE_DATES))}") from e

    # check if desired map scale exists in manifest
    try:
        AVAILABLE_MAPS[map_date][scale]
    except KeyError as e:
        # maps of japan can be generated from prefecture maps
        if scale == "jp":
            return join_prefectures(load_map(date, "jp_pref", quality))
        else:
            raise Exception(
                f"{scale} not available for {map_date}. Available scales: {list(AVAILABLE_MAPS[map_date].keys())}"
            ) from e

    # check if desired map quality exists in manifest
    if quality not in AVAILABLE_MAPS[map_date][scale]:
        # stylized charts can be regenerated from more detailed charts
        if quality == "s":
            if scale == "jp_pref":
                return join_cities(remove_islands(load_map(date, "jp_city_dc", "s")))
            elif scale == "jp":
                return join_prefectures(load_map(date, "jp_pref", "s"))
            else:
                return stylize(load_map(date, scale, "c"))
        else:
            raise Exception(
                f"{quality} not available for {map_date}{scale}. Available qualities: {list(AVAILABLE_MAPS[map_date][scale])}"
            )

    # fetch map
    map_file = fetch_map(map_date, scale, quality)

    map_df = gpd.read_file(map_file)
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

    if scale == "jp_pref":
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

    if scale == "jp_city_dc":
        map_df.loc[map_df["special"] == "designated-city", "city"] = map_df.loc[
            map_df["special"] == "designated-city", "county"
        ]

    if scale == "jp_city_dc" or "jp_city":
        map_df.drop(columns=["id"], errors="ignore", inplace=True)

    try:
        map_df["prefecture"] = map_df["prefecture"].str.replace("沖繩", "沖縄")
        map_df["id"] = map_df["prefecture"].str.replace("沖繩", "沖縄")
    except KeyError:
        pass

    return map_df


# Helper function to add a df to a map
def add_df_to_map(
    df,
    date,
    scale,
    quality="coarse",
):
    map_df = load_map(date, scale=scale, quality=quality)
    map_df = map_df.loc[~map_df["geometry"].is_empty]
    if scale == "jp_pref":
        merge_tokens = ["jp_pref"]
    else:
        map_df = map_df.loc[~(map_df["code"].isnull())]
        map_df = map_df.drop_duplicates(subset="code")
        merge_tokens = ["jp_pref", "code"]
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
