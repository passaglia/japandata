'''
population/data.py

Module which loads, caches, and provides access to population data

Author: Sam Passaglia
'''

import pandas as pd
import numpy as np
import xarray as xr
import os
from collections import OrderedDict

DATA_URL = "https://github.com/passaglia/japandata-sources/raw/main/population/populationdata.tar.gz"
DATA_FOLDER = os.path.join(os.path.dirname(__file__),'populationdata/')

JAPAN_POP_CACHE = os.path.join(os.path.dirname(__file__),'japan_pop.parquet')
PREF_POP_CACHE = os.path.join(os.path.dirname(__file__),'pref_pop.parquet')
LOCAL_POP_CACHE = os.path.join(os.path.dirname(__file__),'local_pop.parquet')

JAPAN_AGE_CACHE = os.path.join(os.path.dirname(__file__),'japan_age.parquet')
PREF_AGE_CACHE = os.path.join(os.path.dirname(__file__),'pref_age.parquet')
LOCAL_AGE_CACHE = os.path.join(os.path.dirname(__file__),'local_age.parquet')

def checkfordata():
    return os.path.exists(DATA_FOLDER)

def getdata():
    if checkfordata():
        print('data already gotten')
        return
    else:
        import urllib.request
        import tarfile
        ftpstream = urllib.request.urlopen(DATA_URL)
        rawfile = tarfile.open(fileobj=ftpstream, mode="r|gz")
        rawfile.extractall(os.path.dirname(__file__))
        return

def load_age_data(year, datalevel='prefecture'):
    assert (datalevel in ['prefecture', 'local'])
    if datalevel=='prefecture':
        assert(1994<=year<=2021)
    elif datalevel=='local':
        assert(1995<=year<=2021)

    fileextension = '.xls'
    skiprows = 2
    if year >= 2021:
        fileextension = '.xlsx'
        skiprows=3

    forced_coltypes = {'code6digit':str, 'prefecture':str}
    cols = ['code6digit', 'prefecture']
    if datalevel=='local': cols+=['city']
    cols += ['gender', 'total-pop']
    if year == 2005: cols+=['total-pop-corrected']
    agebracketmin = 0
    if year < 2015:
        agebracketmax = 80
    else:
        agebracketmax = 100
    while agebracketmin < agebracketmax:
        cols += [(str(int(agebracketmin))+'-'+str(agebracketmin+4))]
        agebracketmin+=5
    cols+= ['>'+str(int(agebracketmax-1))]
    

    ## change the column order to make sure >99 is last
    if datalevel=='prefecture':
        filelabel=str(year)[-2:]+'02'
        if year >= 2013:
            filelabel += 's'
        df = pd.read_excel(DATA_FOLDER+'tnen/'+filelabel+'tnen'+fileextension, skiprows=skiprows,header=None, names=cols, dtype=forced_coltypes)
    elif datalevel=='local':
        filelabel=str(year)[-2:]+'04'
        if year >= 2013:
            filelabel += 's'
        df = pd.read_excel(DATA_FOLDER+'snen/'+filelabel+'snen'+fileextension, skiprows=skiprows,header=None,names=cols, dtype=forced_coltypes)

    if year == 2021: df = df[:-2]

    if datalevel=='local':
        df['city'].replace('\x1f',np.nan,inplace=True)
        df['city'].replace('-', np.nan,inplace=True)
        df['city'] = df['city'].str.strip()
        df['city'] = df['city'].str.replace('*','',regex=False)
        df['city'].replace('', np.nan,inplace=True)

    df['prefecture'] = df['prefecture'].str.strip()
    df['prefecture'] = df['prefecture'].str.replace('*','',regex=False)

    df.loc[df['prefecture']=='合計','code6digit']=np.nan

    if datalevel=='local':
        df.loc[df['city']=='島しょ','code6digit']='133604'
        df.loc[df['city']=='色丹郡色丹村','code6digit']='016951'
        df.loc[df['city']=='国後郡泊村','code6digit']='016969'
        df.loc[df['city']=='国後郡留夜別村','code6digit']='016977'
        df.loc[df['city']=='択捉郡留別村','code6digit']='016985'
        df.loc[df['city']=='紗那郡紗那村','code6digit']='016993'
        df.loc[df['city']=='蘂取郡蘂取村','code6digit']='017001'

    if datalevel=='prefecture':
        #df = df.set_index('prefecture')
        df['code'] = df['code6digit'].apply(lambda s: s if pd.isna(s) else s[:2])
        df.drop(['code6digit'], inplace=True, axis=1)
    if datalevel=='local':
        df['code'] = df['code6digit'].apply(lambda s: s if pd.isna(s) else s[:-1])
        #df = df.set_index('code6digit')

    ##### SELF-CONSISTENCY TESTS ###
    # This tests whether men+women = total
    grouped = df.drop(['code6digit','prefecture','city','gender','total-pop-corrected'],axis=1, errors='ignore').groupby('code')
    def testfunc(group):
        #print(group)
        assert((group.iloc[0,:-1] == group.iloc[1,:-1]+group.iloc[2,:-1]).all())
    grouped.apply(testfunc)
    ##### SELF-CONSISTENCY TESTS ####

    df['unknown'] = df['total-pop'] - df.drop(['code','code6digit','prefecture','total-pop-corrected','city','gender','total-pop'],axis=1,errors='ignore').sum(axis=1)

    df['gender'].replace({'計':'total','男':'men','女':'women'}, inplace=True)
    return df

def load_pop_data(year, datalevel='prefecture'):
    assert (datalevel in ['prefecture', 'local'])
    if datalevel=='prefecture':
        assert(1968<=year<=2021)
    elif datalevel=='local':
        assert(1995<=year<=2021)

    fileextension = '.xls'
    skiprows = 4
    if year >= 2021:
        fileextension = '.xlsx'
        skiprows=6

    forced_coltypes = {'code6digit':str, 'prefecture':str}
    # if datalevel=='local':
    #     forced_coltypes['city'] = 'str'

    cols = ['code6digit', 'prefecture']
    if datalevel=='local': cols+=['city']
    cols += ['men', 'women', 'total-pop']
    if year == 2005: cols+=['total-pop-corrected']
    cols+=['households']
    if 1980 <= year:
        if year == 2005: cols+=['households-corrected']
        if 2013 <= year <= 2021: cols+=['moved-in-domestic', 'moved-in-international']
        cols+=['moved-in',  'born', 'other-in', 'total-in']
        if 2013 <= year <= 2021: cols+=['moved-out-domestic', 'moved-out-international']
        cols+=['moved-out',  'died', 'other-out', 'total-out',
        'in-minus-out']
        if 1994<=year:
            cols+=['in-minus-out-rate']
        cols+=['born-minus-died','born-minus-died-rate','social-in-minus-social-out','social-in-minus-social-out-rate']

    if datalevel=='prefecture':
        filelabel=str(year)[-2:]+'01'
        if year >= 2013:
            filelabel += 's'
        df = pd.read_excel(DATA_FOLDER+'tjin/'+filelabel+'tjin'+fileextension, skiprows=skiprows,header=None, names=cols, dtype=forced_coltypes)
    elif datalevel=='local':
        filelabel=str(year)[-2:]+'03'
        if year >= 2013:
            filelabel += 's'
        df = pd.read_excel(DATA_FOLDER+'sjin/'+filelabel+'sjin'+fileextension, skiprows=skiprows,header=None,names=cols, dtype=forced_coltypes)

    if year == 2021: df = df[:-1]

    if datalevel=='local':
        df['city'].replace('\x1f',np.nan,inplace=True)
        df['city'].replace('-', np.nan,inplace=True)
        df['city'] = df['city'].str.strip()
    
    df['prefecture'] = df['prefecture'].str.strip()
    df.loc[df['prefecture']=='合計','code6digit']=np.nan

    if datalevel=='local':
        df.loc[df['city']=='島しょ','code6digit']='133604'
        df.loc[df['city']=='色丹郡色丹村','code6digit']='016951'
        df.loc[df['city']=='国後郡泊村','code6digit']='016969'
        df.loc[df['city']=='国後郡留夜別村','code6digit']='016977'
        df.loc[df['city']=='択捉郡留別村','code6digit']='016985'
        df.loc[df['city']=='紗那郡紗那村','code6digit']='016993'
        df.loc[df['city']=='蘂取郡蘂取村','code6digit']='017001'

    if datalevel=='prefecture':
        df = df.set_index('prefecture')
        df['code'] = df['code6digit'].apply(lambda s: s if pd.isna(s) else s[:2])
        df.drop(['code6digit'], inplace=True, axis=1)
    if datalevel=='local':
        df['code'] = df['code6digit'].apply(lambda s: s if pd.isna(s) else s[:-1])
        df = df.set_index('code6digit')

    ##### SELF-CONSISTENCY TESTS ####
    assert((df['men']+df['women'] == df['total-pop']).all())
    if year >= 1980:
        assert((df['moved-in']+df['born']+df['other-in'] == df['total-in']).all())
        if year != 1996 and datalevel != 'local':
            assert((df['moved-out']+df['died']+df['other-out'] == df['total-out']).all())
        assert((df['total-in']-df['total-out']==df['in-minus-out']).all())
        assert((df['born']-df['died'] ==df['born-minus-died']).all())
        assert((df['moved-in']+df['other-in']-df['moved-out']-df['other-out'] == df['social-in-minus-social-out']).all())
    if year >= 2013:
        assert((df['moved-in-domestic']+df['moved-in-international'] == df['moved-in']).all())
        assert((df['moved-out-domestic']+df['moved-out-international'] == df['moved-out']).all())
    if datalevel == 'prefecture':
        assert((df.drop(['in-minus-out-rate','born-minus-died-rate','social-in-minus-social-out-rate', 'code6digit', 'code'],axis=1,errors='ignore').drop('合計').sum().values == df.loc[df.index=='合計'].drop(['in-minus-out-rate','born-minus-died-rate','social-in-minus-social-out-rate','code6digit', 'code'],axis=1,errors='ignore').values).all())
    ##### SELF-CONSISTENCY TESTS ####

    return df

def clean_pop_data():
    years = np.arange(1968, 2022)
    df_japan_pop_list= []
    df_pref_pop_list = []
    df_local_pop_list = []
    local_pop_years=[]
    for year in years:
        print(year)
        df_pref_pop = load_pop_data(year, datalevel='prefecture')
        df_japan_pop = df_pref_pop[df_pref_pop.index == '合計'].copy().reset_index(drop=True).drop(['code'],axis=1)
        df_japan_pop_list.append(df_japan_pop)
        df_pref_pop.drop('合計',inplace=True)
        df_pref_pop_list.append(df_pref_pop)

        if year >= 1995:
            local_pop_years.append(year)
            df_local_pop = load_pop_data(year, datalevel='local')

            ### checking consistency of the summary rows of the local table with the summary table
            df_local_pop_pref = (df_local_pop.loc[pd.isna(df_local_pop['city'])])
            df_local_pop_pref.reset_index(inplace=True,drop=True)
            if year!=2013:
                ### checking consistency of the japan table and the local table
                assert((df_japan_pop.values==df_local_pop_pref.drop(['prefecture', 'city','code'], axis=1).iloc[0].values).all())
                ### checking consistency of the prefecture table and the local table
                assert((df_pref_pop.drop(['code'],axis=1).values==df_local_pop_pref.drop(['prefecture', 'city','code'],axis=1).iloc[1:].values).all())
            else:
                assert((df_japan_pop.drop(['in-minus-out-rate','born-minus-died-rate','social-in-minus-social-out-rate'],axis=1).values==df_local_pop_pref.drop(['prefecture', 'city','code','in-minus-out-rate','born-minus-died-rate','social-in-minus-social-out-rate'],axis=1).iloc[0].values).all())
                assert((df_pref_pop.drop(['in-minus-out-rate','born-minus-died-rate','social-in-minus-social-out-rate','code'],axis=1).values==df_local_pop_pref.drop(['in-minus-out-rate','born-minus-died-rate','social-in-minus-social-out-rate','prefecture', 'city','code'],axis=1).iloc[1:].values).all())

            ## dropping the summary rows
            df_local_pop = (df_local_pop.loc[~pd.isna(df_local_pop['city'])])

            print('number of local municipalities', len(df_local_pop))
            df_local_pop_list.append(df_local_pop)
  
    japan_columns = set()
    for df_japan_pop in df_japan_pop_list:
        japan_columns = japan_columns.union(df_japan_pop)
    for df_japan_pop in df_japan_pop_list:
        for column in japan_columns:
            if column not in df_japan_pop.columns:
                df_japan_pop[column] = np.nan

    pref_columns = set()
    for df_pref_pop in df_pref_pop_list:
        pref_columns=pref_columns.union(df_pref_pop.columns)
    for df_pref_pop in df_pref_pop_list:
        for column in pref_columns:
            if column not in df_pref_pop.columns:
                df_pref_pop[column] = np.nan

    local_columns = set()
    for df_local_pop in df_local_pop_list:
        local_columns=local_columns.union(df_local_pop.columns)
    for df_local_pop in df_local_pop_list:
        for column in local_columns:
            if column not in df_local_pop.columns:
                df_local_pop[column] = np.nan

    years -= 1 #The years above are actually the value at the end of the previous fiscal year. This line makes it the value at the end of the current fiscal year.
    local_pop_years = np.array(local_pop_years) - 1 
    japan_pop_array = xr.concat([df_japan_pop.to_xarray() for df_japan_pop in df_japan_pop_list], dim=xr.DataArray(years,dims='year'))
    japan_pop_array = japan_pop_array.drop('index')
    pref_pop_array = xr.concat([df_pref_pop.to_xarray() for df_pref_pop in df_pref_pop_list], dim=xr.DataArray(years,dims='year'))
    local_pop_array = xr.concat([df_local_pop.to_xarray() for df_local_pop in df_local_pop_list], dim=xr.DataArray(local_pop_years,dims='year'))

    return japan_pop_array, pref_pop_array, local_pop_array

def clean_age_data():
    years = np.arange(1994, 2022)
    df_japan_age_list= []
    df_pref_age_list = []
    df_local_age_list = []
    local_age_years=[]
    for year in years:
        print(year)
        df_pref_age = load_age_data(year, datalevel='prefecture')
        df_japan_age = df_pref_age[df_pref_age['prefecture'] == '合計'].copy().reset_index(drop=True).drop(['code','prefecture'],axis=1)
        df_japan_age_list.append(df_japan_age)
        df_pref_age = df_pref_age.set_index('prefecture').drop('合計').reset_index()
        df_pref_age_list.append(df_pref_age)

        if year >= 1995:
            local_age_years.append(year)
            df_local_age = load_age_data(year, datalevel='local')

            ### checking consistency of the summary rows of the local table with the summary table
            df_local_age_pref = (df_local_age.loc[pd.isna(df_local_age['city'])])
            df_local_age_pref.reset_index(inplace=True,drop=True)
            ### checking consistency of the japan table and the local table
            assert((df_japan_age.drop(['gender','total-pop-corrected'],axis=1,errors='ignore').values==df_local_age_pref.drop(['prefecture', 'city','code','code6digit','gender','total-pop-corrected'], axis=1,errors='ignore').iloc[0:3].values).all())
            ### checking consistency of the prefecture table and the local table
            assert((df_pref_age.drop(['code','gender','total-pop-corrected','prefecture'],axis=1,errors='ignore').values==df_local_age_pref.drop(['prefecture', 'city','code','gender','total-pop-corrected','code6digit'],axis=1,errors='ignore').iloc[3:].values).all())
 
            # dropping the summary rows
            df_local_age = (df_local_age.loc[~pd.isna(df_local_age['city'])])
            df_local_age_list.append(df_local_age)
  
    japan_columns = set()
    for df_japan_age in df_japan_age_list:
        japan_columns = japan_columns.union(df_japan_age)
    for df_japan_age in df_japan_age_list:
        for column in japan_columns:
            if column not in df_japan_age.columns:
                df_japan_age[column] = np.nan

    pref_columns = set()
    for df_pref_age in df_pref_age_list:
        pref_columns=pref_columns.union(df_pref_age.columns)
    for df_pref_age in df_pref_age_list:
        for column in pref_columns:
            if column not in df_pref_age.columns:
                df_pref_age[column] = np.nan

    local_columns = set()
    for df_local_age in df_local_age_list:
        local_columns=local_columns.union(df_local_age.columns)
    for df_local_age in df_local_age_list:
        for column in local_columns:
            if column not in df_local_age.columns:
                df_local_age[column] = np.nan

    years -= 1 #The years above are actually the value at the end of the previous fiscal year. This line makes it the value at the end of the current fiscal year.
    local_age_years = np.array(local_age_years) - 1 
    japan_age_array = xr.concat([df_japan_age.to_xarray() for df_japan_age in df_japan_age_list], dim=xr.DataArray(years,dims='year'))
    japan_age_array = japan_age_array.drop('index')
    pref_age_array = xr.concat([df_pref_age.to_xarray() for df_pref_age in df_pref_age_list], dim=xr.DataArray(years,dims='year'))
    local_age_array = xr.concat([df_local_age.to_xarray() for df_local_age in df_local_age_list], dim=xr.DataArray(local_age_years,dims='year'))

    return japan_age_array, pref_age_array, local_age_array

try:
    japan_pop_xr = pd.read_parquet(JAPAN_POP_CACHE).to_xarray()
    prefecture_pop_xr = pd.read_parquet(PREF_POP_CACHE).to_xarray()
    local_pop_xr = pd.read_parquet(LOCAL_POP_CACHE).to_xarray()
except FileNotFoundError:
    if not checkfordata():
        getdata()
    japan_pop_xr, prefecture_pop_xr, local_pop_xr = clean_pop_data()
    (japan_pop_xr.to_dataframe()).to_parquet(JAPAN_POP_CACHE)
    (prefecture_pop_xr.to_dataframe()).to_parquet(PREF_POP_CACHE)
    (local_pop_xr.to_dataframe()).to_parquet(LOCAL_POP_CACHE)

japan_pop_df = japan_pop_xr.to_dataframe().reset_index().drop('index',axis=1).fillna(value=np.nan)
prefecture_pop_df = prefecture_pop_xr.to_dataframe().reset_index().fillna(value=np.nan)
prefecture_pop_df = prefecture_pop_df.drop(prefecture_pop_df.loc[pd.isna(prefecture_pop_df['total-pop'])].index)
local_pop_df = local_pop_xr.to_dataframe().reset_index().fillna(value=np.nan)
local_pop_df = local_pop_df.drop(local_pop_df.loc[pd.isna(local_pop_df['total-pop'])].index)

try:
    japan_age_xr = pd.read_parquet(JAPAN_AGE_CACHE).to_xarray()
    prefecture_age_xr = pd.read_parquet(PREF_AGE_CACHE).to_xarray()
    local_age_xr = pd.read_parquet(LOCAL_AGE_CACHE).to_xarray()
except FileNotFoundError:
    if not checkfordata():
        getdata()
    japan_age_xr, prefecture_age_xr, local_age_xr = clean_age_data()
    (japan_age_xr.to_dataframe()).to_parquet(JAPAN_AGE_CACHE)
    (prefecture_age_xr.to_dataframe()).to_parquet(PREF_AGE_CACHE)
    (local_age_xr.to_dataframe()).to_parquet(LOCAL_AGE_CACHE)

japan_age_df = japan_age_xr.to_dataframe().reset_index().drop('index',axis=1).fillna(value=np.nan)
prefecture_age_df = prefecture_age_xr.to_dataframe().reset_index().fillna(value=np.nan)
prefecture_age_df = prefecture_age_df.drop(prefecture_age_df.loc[pd.isna(prefecture_age_df['total-pop'])].index)
local_age_df = local_age_xr.to_dataframe().reset_index().fillna(value=np.nan)
local_age_df = local_age_df.drop(local_age_df.loc[pd.isna(local_age_df['total-pop'])].index)

## rate in 2013 is blank
# The gaijin file has like soukei but:
    #in other separated into in other (（法第30条の47）),  in other (（国籍喪失）), in other (other), and in other (total)
    # out other (帰化等), out other (other), out other (total)
    # Rate columns are empty for some reason
# The nihonjin file is 
    # has housesold separated into (Japanese, Mixed, Total)
    # in other separated into in other (帰化等), in other (other), in other (total)
    # out other separated into out other (loss of citizenship), out other (other), out other (total)
    # here the rates are not empty

# 94: in addition to the jin table there is a nen table with age and gender breakdown in each prefecture:
#2 rows of header
#3 rows of total
# 21 columns: dantai, todoufuken, gender (total/male/female), total, then age breakdown in 5 year bins until 80+

# Starting in 95: we get a sjin table which separates by locality.
# one row for each locality, plus a total row for each prefecture (check if there is any unassigned pop)
# Starting in 95 we also get a snen table which is the same for the age stuff

### What is the rate for the total row?