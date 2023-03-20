"""
maps/maps.py

Module which provides topojson maps of japan.
Maps from https://geoshape.ex.nii.ac.jp/city/choropleth/

Author: Sam Passaglia
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from japandata.utils import load_dict, logger

CACHE_FOLDER = Path(Path(__file__).parent, "cache/")


"""
File fetching and caching
"""


def fetch_file(fname):
    """Fetches and caches file

    Args:
        fname (Path): name of file to fetch

    Returns:
        Path: cached filepath.
    """

    cached = Path(CACHE_FOLDER, fname)
    if not cached.exists():
        cached.parent.mkdir(
            parents=True, exist_ok=True
        )  # recreate any required subdirectories locally
        logger.info(f"Fetching {fname} for japandata.maps")
        from japandata.download import DOWNLOAD_INFO, download_progress

        url = DOWNLOAD_INFO["maps"]["latest"]["url"] + fname
        download_progress(url, cached)
    return cached


def fetch_manifest():
    """Fetches and caches the map manifest file.

    Returns:
        Path: cached manifest filepath
    """
    # TODO: We are caching the manifest, which prevents us from updating the manifest remotely without asking the user to clear cache or updating the package. In future could implement a check for a new manifest and update the cache if necessary.

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


"""
Map Cleaning Functions for Stylization
"""

# TODO: add docstrings to these functions


def remove_contested(df):
    contested_codes = [
        "01695",
        "01696",
        "01697",
        "01698",
        "01699",
        "01700",
    ]

    df = df[~df["code"].isin(contested_codes)].reset_index(drop=True)

    return df


# def remove_islands(df):
#     island_codes_hokkaido = [
#         "01518",
#         "01519",
#         "01517",
#         "01367",
#         "01695",
#         "01696",
#         "01697",
#         "01698",
#         "01699",
#         "01700",
#     ]
#     island_codes_wakkayama = []  # ['30427']
#     island_codes_ooita = ["44322"]
#     island_codes_yamaguchi = ["35305"]
#     island_codes_tottori = ["32528", "32525", "32526", "32527"]
#     island_codes_tokyo = [
#         "13421",
#         "13361",
#         "13362",
#         "13363",
#         "13364",
#         "13381",
#         "13382",
#         "13401",
#         "13402",
#     ]

#     island_codes = (
#         island_codes_hokkaido
#         + island_codes_wakkayama
#         + island_codes_ooita
#         + island_codes_yamaguchi
#         + island_codes_tottori
#         + island_codes_tokyo
#     )

#     df = df[~df["code"].isin(island_codes)].reset_index(drop=True)

#     return df


def join_cities(city_df):
    from shapely.validation import make_valid

    city_df["geometry"] = city_df["geometry"].apply(make_valid)
    city_df = city_df.drop(
        ["city", "bureau", "county", "code", "special"], axis=1, errors="ignore"
    )
    pref_df = city_df.dissolve("prefecture").reset_index()
    pref_df["geometry"] = (
        pref_df.to_crs("EPSG:30166").buffer(1000).buffer(-1000).to_crs(pref_df.crs)
    )

    return pref_df


def join_prefectures(pref_df):
    from shapely.validation import make_valid

    pref_df["geometry"] = pref_df["geometry"].apply(make_valid)
    japan_df = pref_df.dissolve()
    japan_df["geometry"] = (
        japan_df.to_crs("EPSG:30166").buffer(1000).buffer(-1000).to_crs(japan_df.crs)
    )

    return japan_df


def stylize_city(city_df):
    import shapely
    import topojson as tp

    city_df = remove_contested(city_df)
    city_df = city_df.loc[~city_df["geometry"].is_empty]
    city_df = city_df.loc[~(city_df["code"].isnull())]

    # meters coords
    city_df = city_df.to_crs("EPSG:30166")
    topojson = tp.Topology(city_df, prequantize=False)
    city_df = topojson.toposimplify(
        500,  # this is in meters
        prevent_oversimplify=True,
    ).to_gdf()
    noncont = ["30207", "30203", "20385"]
    city_df.loc[~city_df["code"].isin(noncont), "geometry"] = city_df.loc[
        ~city_df["code"].isin(noncont), "geometry"
    ].apply(
        lambda geometry: shapely.geometry.MultiPolygon(
            [
                P
                for P in geometry.geoms
                if (
                    (P.area > 6000 * 6000)  # meters * meters
                    or (P.area == max([P.area for P in geometry.geoms]))
                )
            ]
        )
    )
    # lat long coords
    city_df = city_df.to_crs("EPSG:6668")
    return city_df


def stylize_pref(pref_df):
    import shapely
    import topojson as tp

    # meters coords
    pref_df = pref_df.loc[~pref_df["geometry"].is_empty]
    pref_df = pref_df.to_crs("EPSG:30166")
    topojson = tp.Topology(pref_df, prequantize=False)
    pref_df = topojson.toposimplify(
        500,  # this is in meters
        prevent_oversimplify=False,
    ).to_gdf()

    noncont = ["和歌山県"]
    pref_df.loc[~pref_df["prefecture"].isin(noncont), "geometry"] = pref_df.loc[
        ~pref_df["prefecture"].isin(noncont), "geometry"
    ].apply(
        lambda geometry: shapely.geometry.MultiPolygon(
            [P for P in geometry.geoms if P.area > 20000 * 20000]
        )
        if geometry.geom_type == "MultiPolygon"
        else geometry
    )
    # lat long coords
    pref_df = pref_df.to_crs("EPSG:6668")

    return pref_df


def stylize_jp(jp_df):
    import shapely
    import topojson as tp

    # meters coords
    jp_df = jp_df.loc[~jp_df["geometry"].is_empty]
    jp_df = jp_df.to_crs("EPSG:30166")
    topojson = tp.Topology(jp_df, prequantize=False)
    jp_df = topojson.toposimplify(
        500,  # this is in meters
        prevent_oversimplify=False,
    ).to_gdf()
    jp_df["geometry"] = [
        shapely.geometry.MultiPolygon(
            [P for P in geometry.geoms if P.area > 1000 * 1000]
        )  # meters * meters
        for geometry in jp_df["geometry"]
    ]
    # lat long coords
    jp_df = jp_df.to_crs("EPSG:6668")

    return jp_df


"""
Map Loading Functions
"""


manifest_file = fetch_manifest()
AVAILABLE_MAPS = load_dict(manifest_file)
AVAILABLE_DATES = [np.datetime64(date) for date in list(AVAILABLE_MAPS.keys())]


def load_and_clean_map_file(map_file):
    # cleaning the map files

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

    map_df.dropna(axis=1, how="all", inplace=True)
    map_df.drop(columns=["id"], errors="ignore", inplace=True)

    try:
        map_df.loc[map_df["special"] == "designated-city", "city"] = map_df.loc[
            map_df["special"] == "designated-city", "county"
        ]
    except KeyError:
        pass

    try:
        map_df["prefecture"] = map_df["prefecture"].str.replace("沖繩", "沖縄")
    except KeyError:
        pass

    try:
        map_df = remove_contested(map_df)
    except KeyError:
        pass

    return map_df


def load_map(date=2022, scale="jp_city_dc", quality="coarse"):
    """Load a map of japan at a given scale and quality.
    Args:
        map_date (datetime64 or str): approximate date of desired map
        scale (str): scale of map to fetch
        quality (str): quality of map to fetch

    Returns:
        geopandas dataframe: topojson map
    """

    # allow for longhand quality arguments
    quality_arg_dict = {
        "stylized": "s",
        "coarse": "c",
        "low": "l",
        "medium": "i",
        "high": "h",
    }
    try:
        quality = quality_arg_dict[quality]
    except KeyError:
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
        # stylized charts can be generated from coarse charts
        if quality == "s":
            if scale == "jp_pref":
                return stylize_pref(join_cities(load_map(date, "jp_city_dc", "s")))
            elif scale == "jp":
                return stylize_jp(join_prefectures(load_map(date, "jp_pref", "s")))
            else:
                return stylize_city(load_map(date, scale, "c"))
        else:
            raise Exception(
                f"{quality} not available for {map_date}{scale}. Available qualities: {list(AVAILABLE_MAPS[map_date][scale])}"
            )

    # fetch map
    map_file = fetch_map(map_date, scale, quality)

    map_df = load_and_clean_map_file(map_file)

    return map_df


# Helper function to merge a DataFrame to a map
def add_df_to_map(
    df,
    date,
    scale,
    quality="coarse",
    indicator=False,
    drop_df_overlap_keys=True,
    clean=False,
):
    map_df = load_map(date, scale, quality)
    if clean:
        map_df = map_df.loc[~map_df["geometry"].is_empty]
    if scale == "jp_pref":
        merge_tokens = ["prefecture"]
    else:
        if clean:
            map_df = map_df.loc[~(map_df["code"].isnull())]
            map_df = map_df.drop_duplicates(subset="code")
        merge_tokens = ["prefecture", "code"]
    merged_df = pd.merge(
        map_df,
        df,
        on=merge_tokens,
        how="left",
        suffixes=["", "_df"],
        indicator=True,
    )
    assert len(merged_df) == len(map_df)

    print(len(merged_df.loc[(merged_df["_merge"] == "left_only")]), "failures")

    if not indicator:
        merged_df = merged_df.drop(columns=["_merge"])

    if drop_df_overlap_keys:
        merged_df = merged_df.drop(
            columns=[col for col in merged_df.columns if col.endswith("_df")],
            errors="ignore",
        )

    merged_df = gpd.GeoDataFrame(merged_df)
    return merged_df
