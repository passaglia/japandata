'''
furusatonouzei/data.py

Module which processes and provides access to data about the chihoukoufuzei tax program.

Author: Sam Passaglia
'''

import pandas as pd
import numpy as np
import xarray as xr
import os
from scipy import stats
import matplotlib.pyplot as plt 
import jaconv
from japandata.readings.data import pref_names_df

#jaconv.z2h(df['code'][0], digit=True) 
DATA_URL = "https://github.com/passaglia/japandata-sources/raw/main/chihoukoufuzei/chihoukoufuzeidata.tar.gz"

CK_DATA_FOLDER = os.path.join(os.path.dirname(__file__),'chihoukoufuzeidata/')
MUNI_FOLDER = os.path.join(CK_DATA_FOLDER,'muni/')
PREF_FOLDER = os.path.join(CK_DATA_FOLDER,'pref/')

MUNI_CACHE_FILE = os.path.join(os.path.dirname(__file__),'muni.parquet')
PREF_CACHE_FILE = os.path.join(os.path.dirname(__file__),'pref.parquet')

def checkfordata():
    return os.path.exists(CK_DATA_FOLDER)

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

def load_muni_ckz(year):

    cols = ['trash','prefecture','city', 'ckz', 'ckz-prev-year']
    fileextension = '.xlsx'
    skiprows = 6
    df = pd.read_excel(MUNI_FOLDER+str(year)+fileextension, skiprows=skiprows, header=None, names=cols)
    df = df.reset_index(drop=True)
    df = df.drop('trash', axis=1)

    currentPref = df.iloc[0,0]
    assert(currentPref == '北海道')
    for index, row in df.iterrows():
        if pd.isna(row.prefecture):
            df.iloc[index,0] = currentPref
        else:
            currentPref = row.prefecture
    
    df['year'] = year
    df['ckz'] = 1000*df['ckz']
    df['ckz-prev-year'] = 1000*df['ckz-prev-year']
    df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"

    df = df.drop(df.loc[pd.isna(df['city'])].index)
    assert(len(df)==1719)

    return df 

def load_muni_income(year):

    fileextension = '.xls'
    skiprows = 6
    if year == 2022:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'income':42}
    elif year in [2021,2020,2019]:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'income':43}
    elif year in [2018,2017,2016,2015]:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'income':38}
    else:
        Exception('year not in allowable')
       
    df = pd.read_excel(MUNI_FOLDER+str(year)+'-income'+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)
    
    df['year'] = year
    df['income'] = 1000*df['income']
    df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"

    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('type',axis=1)
    df['code'] = df['code-str'].apply(lambda s: s[1:6])
    df = df.drop('code-str',axis=1)

    assert(len(df)==1719)

    return df 

def load_muni_demand(year):
    ## The final demand is the actual demand minus the allowable debt issuance. This is what then keeps the final demand minus final income similar to the total amount of money in the pot.

    fileextension = '.xlsx'
    skiprows = 5
    #forced_coltypes = {'code6digit':str, 'prefecture':str}
    if year == 2022:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4, 'demand-pre-debt':53,'special-debt':55, 'final-demand':56}
    elif year in [2021]:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand-pre-debt':55,'special-debt':57,'final-demand':58}
    elif year in [2020]:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand-pre-debt':52,'special-debt':54,'final-demand':55}
    elif year in [2019,2018]:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand-pre-debt':51,'special-debt':53,'final-demand':54}
        fileextension = '.xls'
    elif year in [2017,2016]:
        cols =  {'code-str':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand-pre-debt':52,'special-debt':54,'final-demand':55}
        fileextension = '.xls'
    elif year in [2015]:
        cols =  {'code-str':1,'prefecture':2,'city':3,'type':4, 'deficit-or-surplus':5,'demand-pre-debt':53,'special-debt':55,'final-demand':56}
        fileextension = '.xls'
    else:
        Exception('year not in allowable range')
    
    df = pd.read_excel(MUNI_FOLDER+str(year)+'-demand'+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)
    
    df['year'] = year

    df['final-demand'] = 1000*df['final-demand']
    df['special-debt'] = 1000*df['special-debt']
    df['demand-pre-debt'] = 1000*df['demand-pre-debt']
    df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"
    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('type',axis=1)
    df['code'] = df['code-str'].apply(lambda s: s[1:6])
    df = df.drop('code-str',axis=1)

    assert((np.abs(df['demand-pre-debt']-df['special-debt']-df['final-demand'])<1000).all())

    assert(len(df)==1719)

    return df 

def load_pref_ckz(year):

    fileextension = '.xls'
    skiprows = 5
    #forced_coltypes = {'code6digit':str, 'prefecture':str}
    J=9
    if year in [2020,2019,2018,2017,2016,2015]:
        cols =  {'code-prefecture':0, 'final-demand':3, 'income':6, 'ckz':9}
    else:
        cols =  {'code-prefecture':0, 'final-demand':3, 'income':6, 'ckz':9}
       
    df = pd.read_excel(PREF_FOLDER+str(year)+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)

    df[['code','prefecture']] = df['code-prefecture'].str.extract('(?P<code>\d{1,})(?P<prefecture>.*)')

    df['year'] = year
    df['income'] = 1000*df['income']
    df['ckz'] = 1000*df['ckz']
    df['final-demand'] = 1000*df['final-demand']

    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('code-prefecture',axis=1)
    df['code'] = df['code'].apply(lambda s: jaconv.z2h(s, digit=True) )
    df['prefecture'] = df['prefecture'].str.replace('\u3000','')

    assert(len(df)==47)
    return df 

def load_pref_income(year):

    fileextension = '.xls'
    skiprows = 6
    if year == 2022:
        cols =  {'code-prefecture':0, 'income':42}
    elif year in [2021,2020]:
        cols =  {'code-prefecture':0,'income':43}
    elif year in [2019]:
        cols =  {'code-prefecture':1,'income':46}
    elif year in [2018]:
        cols =  {'code-prefecture':1,'income':39}
    elif year in [2017]:
        cols =  {'code-prefecture':1,'income':40}
    elif year in [2016,2015]:
        cols =  {'code-prefecture':1,'income':41}
    else:
        Exception('year not in allowable range')
       
    df = pd.read_excel(PREF_FOLDER+str(year)+'-income'+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)

    df[['code','prefecture']] = df['code-prefecture'].str.extract('(?P<code>\d{1,})(?P<prefecture>.*)')

    df['year'] = year
    df['income'] = 1000*df['income']

    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('code-prefecture',axis=1)

    df['code'] = df['code'].apply(lambda s: jaconv.z2h(s, digit=True) )

    def apply_todoufuken(pref):
        if pref in ['東京']:
            return pref + '都'
        elif pref in ['京都','大阪']:
            return pref + '府'
        elif pref in ['北海道']:
            return pref 
        else:
            return pref+'県'
    df['prefecture'] = df['prefecture'].str.strip().str.replace(' ','').apply(apply_todoufuken)

    assert(len(df)==47)
    return df 

def load_pref_demand(year, revised=False):
    ## The final demand is the actual demand minus the allowable debt issuance. This is what then keeps the final demand minus final income similar to the total amount of money in the pot.

    fileextension = '.xlsx'
    skiprows = 5
    #forced_coltypes = {'code6digit':str, 'prefecture':str}
    if year in [2022]:
        cols =  {'code-prefecture':0, 'demand-pre-debt':45,'special-debt':47, 'final-demand':48}
    if year in [2021]:
        if revised:
            cols =  {'code-prefecture':0, 'demand-pre-debt':47,'special-debt':49, 'final-demand':50}
        else:
            cols =  {'code-prefecture':0, 'demand-pre-debt':45,'special-debt':47, 'final-demand':48}
    elif year in [2020]:
        cols =  {'code-prefecture':0, 'demand-pre-debt':44,'special-debt':46, 'final-demand':47}
    elif year in [2019,2018]:
        cols =  {'code-prefecture':1, 'demand-pre-debt':44,'special-debt':46, 'final-demand':47}
    elif year in [2017,2016,2015]:
        cols =  {'code-prefecture':1, 'demand-pre-debt':45,'special-debt':47, 'final-demand':48}
    else:
        Exception('year not in allowable range')
    
    if revised:
        fileextension='-revised'+fileextension

    df = pd.read_excel(PREF_FOLDER+str(year)+'-demand'+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)
    
    
    df['year'] = year
    df['final-demand'] = 1000*df['final-demand']
    df['special-debt'] = 1000*df['special-debt']
    df['demand-pre-debt'] = 1000*df['demand-pre-debt']
    df[['code','prefecture']] = df['code-prefecture'].str.extract('(?P<code>\d{1,})(?P<prefecture>.*)')
    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('code-prefecture',axis=1)
    df['code'] = df['code'].apply(lambda s: jaconv.z2h(s, digit=True) )

    def apply_todoufuken(pref):
        if pref in ['東京']:
            return pref + '都'
        elif pref in ['京都','大阪']:
            return pref + '府'
        elif pref in ['北海道']:
            return pref 
        else:
            return pref+'県'
    df['prefecture'] = df['prefecture'].str.strip().str.replace(' ','').apply(apply_todoufuken)
    df = df.reset_index(drop=True)


    assert((np.abs(df['demand-pre-debt']-df['special-debt']-df['final-demand'])<1000).all())
    assert(len(df)==47)

    return df 

def load_pref_all():

    known_adjustment_factors = {2020: 0.000510886}
    estimated_adjustment_factors = {2020: 0.0005113}

    years = np.arange(2015, 2023)
    #years = np.arange(2015, 2021)
    df_income = pd.DataFrame()
    df_demand = pd.DataFrame()
    df_ckz = pd.DataFrame()
    for year in years:
        print(year)
        df_income_year = load_pref_income(year)
        df_income = pd.concat([df_income, df_income_year], ignore_index=True)
        df_demand_year = load_pref_demand(year)
        df_demand = pd.concat([df_demand, df_demand_year], ignore_index=True)
        ## The ckz data doesn't go as far as the income/demand de
        try:
            df_ckz_year = load_pref_ckz(year)
            assert((np.abs(1-df_ckz_year['income']/df_income_year['income'])<0.002).all())
            assert((np.abs(1-df_ckz_year['final-demand']/(df_demand_year['final-demand']))<0.005).all())
        except FileNotFoundError:
            df_ckz_year = df_ckz_year.assign(**{column:np.nan for column in df_ckz_year.columns if column not in ['prefecture','code']})
            df_ckz_year['year'] = year
        df_ckz = pd.concat([df_ckz, df_ckz_year], ignore_index=True)

    df = df_income.merge(df_demand, on=['year','code', 'prefecture'],validate='one_to_one')
    df = df.merge(df_ckz, on=['prefecture','year','code'], suffixes=['','_ckzfile'])

    from japandata.indices.data import pref_ind_df 

    pref_ind_df = pref_ind_df.loc[pref_ind_df['year']>(np.min(years)-4)]

    for pref in pref_ind_df.prefecture.unique():
        prefLoc = pref_ind_df.prefecture == pref
        pref_ind_df.loc[prefLoc,'economic-strength-index-prev3yearavg'] = pref_ind_df.loc[prefLoc].sort_values(by='year')[['economic-strength-index','year']].rolling(on='year', window=3,closed='left').mean()['economic-strength-index']
        latest_row = pref_ind_df.loc[(prefLoc) & (pref_ind_df.year == np.max(pref_ind_df.loc[prefLoc].year))].reset_index(drop=True)
        extra_row = pd.DataFrame(np.nan, index=[0],columns= latest_row.columns)
        extra_row['year'] = latest_row['year']+1
        extra_row['prefecture'] = latest_row['prefecture']
        extra_row['economic-strength-index-prev3yearavg'] = np.mean(pref_ind_df.loc[prefLoc].sort_values(by='year')['economic-strength-index'].values[-3:])

        pref_ind_df = pd.concat([pref_ind_df,extra_row],ignore_index=True)

    ## Adding in new rows for years with chihoukoufuzei data but no indices data
    empty_data_template = pref_ind_df.loc[pref_ind_df['year']==np.max(pref_ind_df.year)].copy().assign(**{column:np.nan for column in pref_ind_df.columns if column not in ['prefecture','code']})
    for year in years:
        if year not in pref_ind_df.year.unique():
            pref_ind_df = pd.concat([pref_ind_df,empty_data_template.assign(year=year)])

    df =  df.merge(pref_ind_df,on=['prefecture','year'],validate='one_to_one')
    
    debt_constants = []
    for year in years:
        print(year)
        yeardf = df.loc[df['year']==year]
        print('total shortfall pre debt', ((yeardf['demand-pre-debt']- yeardf['income']).loc[(yeardf['demand-pre-debt']- yeardf['income'])>0].sum())/10**(12), 'trillion yen')
        print('total shortfall post debt', ((yeardf['final-demand']- yeardf['income']).loc[(yeardf['final-demand']- yeardf['income'])>0].sum())/10**(12), 'trillion yen')
        print("total debt", yeardf['special-debt'].sum()/10**12, 'trillion yen')
        if year in known_adjustment_factors.keys():
            ckz = (yeardf['final-demand']*(1-known_adjustment_factors[year])- yeardf['income'])
            ckz.iloc[np.where(ckz<0)[0]] = 0
            print('LAT computed', ckz.sum()/10**12, 'trillion yen')
        
        if year < 2022:
            plt.close('all')
            if year != 2021:
                print('Special Debt / (demand-pre-debt - income) / (economic strength)')
                plt.hist(yeardf['special-debt']/(yeardf['demand-pre-debt']-yeardf['income'])/yeardf['economic-strength-index'],bins=50,color='red')
            print('Special Debt / (demand-pre-debt - income) / (economic strength average of previous 3 years)')
            # Formula for debt: Deficit * 0.1664 * Zaiseiryoku past 3 year average * extra factor
            plt.hist(yeardf['special-debt']/(yeardf['demand-pre-debt']-yeardf['income'])/yeardf['economic-strength-index-prev3yearavg'],bins=50, color='blue',zorder=-10)  

            print('Special Debt / (demand-pre-debt) / (economic strength pre 3 years)')
            plt.hist(yeardf['special-debt']/(yeardf['demand-pre-debt'])/yeardf['economic-strength-index-prev3yearavg'],bins=50, color='green',zorder=-10)  
            #plt.show()
            # Find the constant 
            scaling_factors = yeardf['special-debt']/(yeardf['demand-pre-debt']-yeardf['income'])/yeardf['economic-strength-index-prev3yearavg']
            scaling_factors = scaling_factors.loc[~scaling_factors.isna()]
            n, bins = np.histogram(scaling_factors,bins=50)
            mode = (bins[np.argmax(n)+1]+bins[np.argmax(n)])/2
            print('debt constant', mode)
            debt_constants.append(mode)

        #print('LAT computed', yeardf['ckz'].sum()/10**12, 'trillion yen')
        #print("total debt as a fraction of total ckz", yeardf['special-debt'].sum()/yeardf['ckz'].sum())
    return df

def pref_tests(df):
    ## TODO: write the tests
    pass
    #df 
    # (6.0966*10**11 + 531845326000.0)/1142089285000.0
    # assert(len(df) == len(df_withind))
    # df.loc[~df['prefecture'].isin(df_withind['prefecture'])]
    # df.loc[~df['year'].isin(df_withind['year'])]

def load_muni_all():
    ## TODO: write the tests

    years = np.arange(2015, 2023)
    #years = [2020]
    df_ckz = pd.DataFrame()
    df_income = pd.DataFrame()
    df_demand = pd.DataFrame()
    for year in years:
        df_ckz_year = load_muni_ckz(year)
        df_ckz = pd.concat([df_ckz,df_ckz_year],ignore_index=True)
        df_income_year = load_muni_income(year)
        df_income = pd.concat([df_income, df_income_year], ignore_index=True)
        df_demand_year = load_muni_demand(year)
        df_demand = pd.concat([df_demand, df_demand_year], ignore_index=True)

    for year in years[0:-1]:
        ckz = df_ckz.loc[df_ckz['year'] == year,'ckz']
        nextyearprevyearckz = df_ckz.loc[df_ckz['year'] == year+1,'ckz-prev-year']
        assert(np.sum(ckz-nextyearprevyearckz) < 1)
    df_ckz = df_ckz.drop('ckz-prev-year', axis=1)

    df = df_income.merge(df_demand, on=['year','code', 'prefecture', 'city','deficit-or-surplus'],validate='one_to_one')

    df= df.merge(df_ckz, on=['prefecture', 'city','year'],validate='one_to_one')

    ### Getting the economic-strength data, which is a key factor in the system
    from japandata.indices.data import local_ind_df

    local_ind_df = local_ind_df.loc[local_ind_df['year']>(np.min(years)-4)]

    ## adding a new row for special wards of tokyo
    for year in local_ind_df.year.unique():
        relevant_cities = local_ind_df.loc[(local_ind_df['year']==year) & (local_ind_df['prefecture'] == '東京都') & (local_ind_df['city'].str.contains('区'))]
        assert(len(relevant_cities)==23)
        mean = relevant_cities.mean(numeric_only=True).drop(['year'])
        new_row = pd.DataFrame(np.nan, index=[0],columns= local_ind_df.columns)
        new_row['year'] = year
        new_row['prefecture'] = '東京都'
        new_row['city'] = '特別区'
        new_row['code'] = '13100'
        for index in mean.index:
            new_row[index] = mean[index]
        local_ind_df = pd.concat([local_ind_df,new_row],ignore_index=True)

    ## Adding the trailing economic strength average
    for code in local_ind_df.code.unique():
        codeLoc = local_ind_df.code == code
        local_ind_df.loc[codeLoc,'economic-strength-index-prev3yearavg'] = local_ind_df.loc[codeLoc].sort_values(by='year')[['economic-strength-index','year']].rolling(on='year', window=3,closed='left').mean()['economic-strength-index']
        latest_row = local_ind_df.loc[(codeLoc) & (local_ind_df.year == np.max(local_ind_df.loc[codeLoc].year))].reset_index(drop=True)
        extra_row = pd.DataFrame(np.nan, index=[0],columns= latest_row.columns)
        extra_row['year'] = latest_row['year']+1
        extra_row[['prefecture','code','city']] = latest_row[['prefecture','code','city']] 
        extra_row['economic-strength-index-prev3yearavg'] = np.mean(local_ind_df.loc[codeLoc].sort_values(by='year')['economic-strength-index'].values[-3:])

        local_ind_df = pd.concat([local_ind_df,extra_row],ignore_index=True)

    ## Adding in new rows for years with chihoukoufuzei data but no indices data
    empty_data_template = local_ind_df.loc[local_ind_df['year']==np.max(local_ind_df.year)].copy().assign(**{column:np.nan for column in local_ind_df.columns if column not in ['prefecture','code','city']})
    for year in years:
        if year not in local_ind_df.year.unique():
            local_ind_df = pd.concat([local_ind_df,empty_data_template.assign(year=year)])

    df_withind =  df.merge(local_ind_df,on=['prefecture','code','year'],validate='one_to_one')
    assert(len(df_withind) == len(df))
    df = df_withind

    adjustment_factors_list = []
    debt_constants = []
    #years = [2020]
    known_adjustment_factors = {'2020': 0.000510886}
    for year in years:
        print(year)
        yeardf = df.loc[df['year']==year]
        print('total shortfall pre debt', ((yeardf['demand-pre-debt']- yeardf['income']).loc[(yeardf['demand-pre-debt']- yeardf['income'])>0].sum())/10**(12), 'trillion yen')
        print('total shortfall post debt', ((yeardf['final-demand']- yeardf['income']).loc[(yeardf['final-demand']- yeardf['income'])>0].sum())/10**(12), 'trillion yen')
        print('total LAT', yeardf['ckz'].sum()/10**12, 'trillion yen')
        print("total debt", yeardf['special-debt'].sum()/10**12, 'trillion yen')
        print("total debt as a fraction of total ckz", yeardf['special-debt'].sum()/yeardf['ckz'].sum())

        adjustment_factors =  np.round(1-(yeardf['ckz']+yeardf['income'])/yeardf['final-demand'],6)
        m = stats.mode(adjustment_factors.iloc[np.where(adjustment_factors>0)])
        adjustment_factors_list.append(m)
        print("adjustment factor", m)

        frac_compensated =  (yeardf['ckz']/(yeardf['final-demand']-yeardf['income'])).values
        print("median compensation fraction after debt", np.median(frac_compensated[np.where(frac_compensated>0)]))
        print("debt as fraction of pre-debt demand", (yeardf['special-debt']/yeardf['demand-pre-debt']).median())
        print("debt as fraction of pre-debt need", (yeardf['special-debt'].sum()/(yeardf['demand-pre-debt']-yeardf['income']).sum()))
        print("debt as fraction of debt + total amount", (yeardf['special-debt'].sum()/(yeardf['special-debt'].sum()+yeardf['ckz'].sum()))) ## This is the closest thing to the 0.1664 number for 2020

        #yeardf = yeardf.merge(local_ind_df,on=['code','year'],validate='one_to_one')
        #yeardf.loc[~yeardf['code'].isin(yeardf['code'])] ## should just be the 23 ku row in here
        print('no ckz places ', np.sum(yeardf['ckz'] <1))
        print('no debt places ', np.sum(yeardf['special-debt'] == 0))
        if year < 2022:
            if year != 2021:
                print('Special Debt / (demand-pre-debt - income) / (economic strength)')
                plt.hist(yeardf['special-debt']/(yeardf['demand-pre-debt']-yeardf['income'])/yeardf['economic-strength-index'],bins=200,color='red')
            print('Special Debt / (demand-pre-debt - income) / (economic strength average of previous 3 years)')
            # Formula for debt: Deficit * 0.1664 * Zaiseiryoku past 3 year average * extra factor
            plt.hist(yeardf['special-debt']/(yeardf['demand-pre-debt']-yeardf['income'])/yeardf['economic-strength-index-prev3yearavg'],bins=200, color='blue',zorder=-10)  
            plt.hist(yeardf['special-debt']/(yeardf['demand-pre-debt'])/yeardf['economic-strength-index-prev3yearavg'],bins=200, color='green',zorder=-10)  
            plt.show()
            # Find the constant 
            scaling_factors = yeardf['special-debt']/(yeardf['demand-pre-debt']-yeardf['income'])/yeardf['economic-strength-index-prev3yearavg']
            scaling_factors = scaling_factors.loc[~scaling_factors.isna()]
            n, bins = np.histogram(scaling_factors,bins=200)
            mode = (bins[np.argmax(n)+1]+bins[np.argmax(n)])/2
            print('debt constant', mode)
            debt_constants.append(mode)
        plt.close('all')

    plt.plot(df.groupby(by='year')['income'].sum()/df.groupby(by='year')['demand-pre-debt'].sum())
    #plt.show()
    plt.close('all')
    plt.plot((df.groupby(by='year')['special-debt'].sum())/df.groupby(by='year')['ckz'].sum())
    #plt.show()
    plt.close('all')
    
    return df
    
 
## Compare FN amount to some of these amounts.
## Understand if the debt is really like just a 'gift' to municipalities? i.e. who buys it, how does it get payed back, how much debt do the municipalities actually take on?

def municipal_tests():
    ## compare to https://www.pref.osaka.lg.jp/attach/2413/00331407/R2_koufuzei.pdf
    osaka2020df = df.loc[(df['year'] == 2020) & (df['city']=='大阪市')]
    assert(osaka2020df['final-demand'].values == 654898101*1000)
    assert(osaka2020df['income'].values == 621727850*1000)
    assert(osaka2020df['ckz'].values == 32835673*1000)
    adjustment_factor = 1-(osaka2020df['ckz']+osaka2020df['income'])/osaka2020df['final-demand']
    assert(np.round(adjustment_factor.values[0],9)==0.000510886)
    ## do i have the sakugo no column?
    sakai2020df = df.loc[(df['year'] == 2020) & (df['city']=='堺市')]
    assert(sakai2020df['final-demand'].values == 169411859*1000)
    assert(sakai2020df['income'].values == 136809228*1000)

    ## I'm missing the 2 sakugo columns. probably this is why the cities don't all agree on the adjustment factor.

    # yeardf.loc[yeardf['city_x'] == '大阪市','special-debt']
    # yeardf.loc[yeardf['city_x'] == '神戸市','economic-strength-index-prev3yearavg']
    # yeardf.loc[yeardf['city_x'] == '加西市','economic-strength-index-prev3yearavg']
    # yeardf.loc[yeardf['city_x'] == '神戸市']['demand-pre-debt']-yeardf.loc[yeardf['city_x'] == '神戸市']['income']
    # yeardf.loc[yeardf['city_x'] == '神戸市']['ckz']+yeardf.loc[yeardf['city_x'] == '神戸市']['special-debt']
    # yeardf.loc[yeardf['city_x'] == '神戸市']['final-demand']+yeardf.loc[yeardf['city_x'] == '神戸市']['special-debt']
    # local_ind_df.loc[(local_ind_df['city'] == '加西市'), ['year','economic-strength-index']].sort_values(by='year')
    # df_all = df_income.merge(df.drop_duplicates(), on=['year','code', 'prefecture', 'city','deficit-or-surplus'], 
    #                    how='left', indicator=True)
    # df_all[df_all['_merge'] == 'left_only']
    # df_income.loc[(df_income['year']==2020) & (df_income['code']=='C282219110000')]
    # df_income.loc[(df_income['city']=='丹波篠山市')]
    # df_demand.loc[(df_demand['year']==2020) & (df_demand['code']=='C282219110000')]
    # df_demand.loc[df_demand['city']=='篠山市']
    # df_income.loc[df_income['city']=='篠山市']
    # assert(len(df) == len(df_income))
    # df_income.loc[df_income['year'].(df['city'] & ~df_income['year'].isin(df['city'])]

try:
    local_df = pd.read_parquet(MUNI_CACHE_FILE)
    pref_df = pd.read_parquet(PREF_CACHE_FILE)
except FileNotFoundError:
    print('getting data')
    if not checkfordata():
        getdata()
    print('loading muni data')
    local_df = load_muni_all()
    local_df.to_parquet(MUNI_CACHE_FILE)
    print('loading pref data')
    pref_df = load_pref_all()
    pref_df.to_parquet(PREF_CACHE_FILE)
    local_df = pd.read_parquet(MUNI_CACHE_FILE)
    pref_df = pd.read_parquet(PREF_CACHE_FILE)

## TODO: move the charts to a different script?