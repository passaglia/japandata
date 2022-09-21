'''
maps/data.py

Module which loads, caches, and provides access to maps of japan

Author: Sam Passaglia
'''

import geopandas as gpd
import os
import numpy as np
import urllib.request
import shutil
from urllib.error import HTTPError

CACHE_FOLDER = os.path.join(os.path.dirname(__file__),'cache/')

ORIGINAL_URL = "https://geoshape.ex.nii.ac.jp/city/topojson/"
MIRROR_URL = "https://github.com/passaglia/japandata-sources/raw/main/map/"
BASE_URL = MIRROR_URL

available_dates = np.array(['1920-01-01','1950-10-01','1955-10-01','1960-10-01','1965-10-01','1970-10-01','1975-10-01','1980-10-01','1985-10-01', '1995-10-01','2000-10-01','2005-01-01','2006-01-01','2007-04-01', '2007-10-01', '2009-03-20', '2010-03-29','2011-03-31','2012-04-01','2013-04-01','2014-04-01', '2015-01-01','2016-01-01','2017-01-01','2018-01-01','2019-01-01','2020-01-01','2021-01-01'], dtype='datetime64')

extension_dict = {'local_dc': '.topojson', 'local': '.topojson', 'prefecture':'.topojson', 'japan':'.geojson'}
level_filename_dict = {'local_dc': 'jp_city_dc', 'local': 'jp_city', 'prefecture':'jp_pref', 'japan':'jp'}
quality_suffix_dict = {"coarse":'c', "low":'l', "medium":'i', "high":'h'}

def load_map(date = 2022, level = 'local_dc', quality='coarse'):
    try:
        date = np.datetime64(date)
    except ValueError:
        date = np.datetime64(str(date)+'-12-31')
    assert (date>= np.min(available_dates))
    assert quality in ["stylized", "coarse", "low", "medium", "high"]
    assert level in ['japan', 'prefecture', 'local', 'local_dc']
    if level == 'japan' and quality not in ['stylized','coarse']:
        print('japan only available at stylized or coarse level')
        return load_map(date,'japan','coarse')
    if level == 'prefecture' and quality == 'stylized':
        return join_localities(remove_islands(load_map(date,'local_dc', 'stylized')))
    if level == 'japan' and quality == 'stylized':
        return join_prefectures(load_map(date,'prefecture', 'stylized'))

    if quality == 'stylized':
        needed_file = level_filename_dict[level]+'.stylized.json'
    else:
        needed_date = str(np.max(available_dates[np.where(date - available_dates >= np.timedelta64(0))]))
        needed_file = needed_date.replace('-','')+"/" + level_filename_dict[level] + '.' + quality_suffix_dict[quality] + extension_dict[level]

    url = BASE_URL+needed_file

    if os.path.exists(CACHE_FOLDER+needed_file):
        map_df = gpd.read_file(CACHE_FOLDER+needed_file)
    else:
        print('cache not found. fetching from server')
        try:
            response = urllib.request.urlopen(url)
            data = response.read()      # a `bytes` object
            text = data.decode('utf-8')
            os.makedirs(os.path.dirname(CACHE_FOLDER+needed_file), exist_ok=True)
            with open(CACHE_FOLDER+needed_file, 'w') as f:
                f.write(text)
        except HTTPError:
            print('file not found on server')
            if level == 'japan':
                print('trying to generate japan file from prefectures')
                from shapely.ops import unary_union
                from shapely.validation import make_valid
                pref_df = load_map(date=needed_date,level='prefecture', quality=quality)
                polygonlist = []
                for i in range(len(pref_df)):
                    print(i)
                    polygonlist.append(make_valid(pref_df.geometry[i]))
                map_df =gpd.GeoDataFrame(geometry=[unary_union(polygonlist)], crs=pref_df.crs)
                map_df.to_file(CACHE_FOLDER+needed_file, driver='GeoJSON')
            else:
                return Exception("Map was not found")
        map_df  = gpd.read_file(CACHE_FOLDER+needed_file)

    map_df.crs = 'EPSG:6668'

    #column headers are explained at https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N03-v2_2.html
    map_df.rename(columns={'N03_001':'prefecture', 'N03_002':'bureau', 'N03_003':'county', 'N03_004':'city', 'N03_005':'founding_date', 'N03_006':'extinction_date','N03_007':'code', 'type':'special'}, inplace=True, errors='ignore')

    if level=='prefecture':
        map_df.drop(columns=['id','bureau', 'founding_date', 'extinction_date', 'city','county'],errors='ignore',inplace=True)
    
    if level == 'local_dc':
        map_df.loc[map_df['special'] == 'designated-city', 'city'] = map_df.loc[map_df['special'] == 'designated-city', 'county']

    if level == 'local_dc' or 'local':
        map_df.drop(columns=['id'],errors='ignore',inplace=True)

    return map_df

def remove_islands(local_df):
    island_codes_hokkaido = ['01518', '01519','01517', '01367', '01695', '01696', '01697', '01698','01699', '01700']
    island_codes_wakkayama = [] #['30427']
    island_codes_ooita = ['44322']
    island_codes_yamaguchi = ['35305']
    island_codes_tottori = ['32528','32525','32526','32527']
    island_codes_tokyo = ['13421','13361','13362','13363','13364','13381','13382','13401','13402']

    island_codes = island_codes_hokkaido+island_codes_wakkayama+island_codes_ooita+island_codes_yamaguchi+island_codes_tottori+island_codes_tokyo

    return local_df.drop(local_df.loc[local_df['code'].isin(island_codes)].index).reset_index(drop=True)

def join_localities(local_df):
    from shapely.ops import unary_union
    from shapely.validation import make_valid
    local_df=local_df.drop(['city','bureau','county','code'],axis=1)
    prefs = []
    polygons = []
    for prefecture in local_df['prefecture'].unique():
        polygonlist = []
        for geometry in local_df.loc[local_df['prefecture']==prefecture,'geometry']:
            polygonlist.append(make_valid(geometry))
        polygons.append(unary_union(polygonlist))
        prefs.append(prefecture)
    
    pref_df = gpd.GeoDataFrame(prefs, columns=['prefecture'],geometry=polygons, crs=local_df.crs )
    return pref_df

def join_prefectures(pref_df):
    from shapely.ops import unary_union
    from shapely.validation import make_valid
    polygonlist = []
    for i in range(len(pref_df)):
        polygonlist.append(make_valid(pref_df.geometry[i]))
    japan_df = gpd.GeoDataFrame(geometry=[unary_union(polygonlist)], crs=pref_df.crs)
    return japan_df

def get_dates():
    return available_dates

def clear_cache():
    shutil.rmtree(CACHE_FOLDER)

def generate_cache(levels=['local_dc'],qualities=['coarse']):
    for level in levels:
        print(level)
        for quality in qualities:
            print(quality)
            for date in available_dates:
                print(date)
                load_map(date, level, quality)

# local_df = remove_islands(load_map(date=2022, level='local_dc', quality='stylized'))
# pref_df = load_map(2022,'prefecture', 'stylized')
# japan_df = load_map(2022,'japan', 'stylized')
# japan_df2 = load_map(2022,'japan', 'coarse')

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
