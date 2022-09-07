'''
furusatonouzei/data.py

Module which processes and provides access to data about the chihoukoufuzei tax program.

Author: Sam Passaglia
'''

import pandas as pd
import numpy as np
import xarray as xr
import os

DATA_URL = "https://github.com/passaglia/japandata-sources/raw/main/chihoukoufuzei/chihoukoufuzeidata.tar.gz"

CK_DATA_FOLDER = os.path.join(os.path.dirname(__file__),'chihoukoufuzeidata/')

# CACHED_FILE = os.path.join(os.path.dirname(__file__),'cleandata.parquet')
# ROUGH_CACHED_FILE = os.path.join(os.path.dirname(__file__),'roughdata.parquet')

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

def load_ckz_rough(year):

    cols = ['trash','prefecture','city', 'ckz', 'ckz-prev-year']
    fileextension = '.xlsx'
    skiprows = 6
    #forced_coltypes = {'code6digit':str, 'prefecture':str}
    df = pd.read_excel(CK_DATA_FOLDER+str(year)+fileextension, skiprows=skiprows, header=None, names=cols)
    df = df.reset_index(drop=True)
    #dtype=forced_coltypes)
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
    
    df = df.drop(df.loc[pd.isna(df['city'])].index)
    assert(len(df)==1719)

    return df 

def load_ckz_income(year):

    fileextension = '.xls'
    skiprows = 6
    #forced_coltypes = {'code6digit':str, 'prefecture':str}
    if year == 2022:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'income':42}
    elif year in [2021,2020,2019]:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'income':43}
    elif year in [2018,2017,2016,2015]:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'income':38}
    else:
        Exception('year not in allowable')
       
    df = pd.read_excel(CK_DATA_FOLDER+str(year)+'-income'+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)
    
    df['year'] = year
    df['income'] = 1000*df['income']
    df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"

    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('type',axis=1)
    
    assert(len(df)==1719)

    return df 

def load_ckz_demand(year):
    fileextension = '.xlsx'
    skiprows = 5
    #forced_coltypes = {'code6digit':str, 'prefecture':str}
    if year == 2022:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand':56}
    elif year in [2021]:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand':58}
    elif year in [2020]:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand':55}
    elif year in [2019,2018,2017,2016]:
        cols =  {'code':0,'prefecture':1,'city':2,'type':3, 'deficit-or-surplus':4,'demand':54}
        fileextension = '.xls'
    elif year in [2015]:
        cols =  {'code':1,'prefecture':2,'city':3,'type':4, 'deficit-or-surplus':5,'demand':56}
        fileextension = '.xls'
    else:
        Exception('year not in allowable range')
    
    df = pd.read_excel(CK_DATA_FOLDER+str(year)+'-demand'+fileextension, skiprows=skiprows, header=None,  usecols=cols.values(), names=cols.keys())
    df = df.reset_index(drop=True)
    
    df['year'] = year
    df['demand'] = 1000*df['demand']
    df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"

    df = df.drop(df.loc[pd.isna(df['prefecture'])].index)
    df = df.drop('type',axis=1)
    assert(len(df)==1719)

    return df 


#def clean_data():
years = np.arange(2015, 2023)
df_rough = pd.DataFrame()
for year in years:
    df_rough_year = load_ckz_rough(year)
    df_rough = pd.concat([df_rough,df_rough_year],ignore_index=True)

for year in years[0:-1]:
    ckz = df_rough.loc[df_rough['year'] == year,'ckz']
    nextyearprevyearckz = df_rough.loc[df_rough['year'] == year+1,'ckz-prev-year']
    assert(np.sum(ckz-nextyearprevyearckz) < 1)

df_rough = df_rough.drop('ckz-prev-year', axis=1)
df_income = pd.DataFrame()
df_demand = pd.DataFrame()
for year in years:
    df_income_year = load_ckz_income(year)
    df_income = pd.concat([df_income, df_income_year], ignore_index=True)
    df_demand_year = load_ckz_demand(year)
    df_demand = pd.concat([df_demand, df_demand_year], ignore_index=True)

df = df_income.merge(df_demand, on=['year','code', 'prefecture', 'city','deficit-or-surplus'],validate='one_to_one')

## todo: merge df and dfrough. i don't believe there's any redundant info
for year in years:
    yeardf = df.loc[df['year']==year]
    yeardf_rough = df_rough.loc[df_rough['year']==year]
    np.max((yeardf['demand']-yeardf['income'])/yeardf['demand'])
    np.min((yeardf['demand']-yeardf['income'])/yeardf['demand'])

    yeardf_rough['ckz'] -  (yeardf['demand']-yeardf['income']).values
    adjustment_factor =  1-(yeardf_rough['ckz']+yeardf['income'])/(yeardf['demand'])
    adjustment_factor =  -(yeardf_rough['ckz']-yeardf['demand'])/yeardf['income']-1

## compare to https://www.pref.osaka.lg.jp/attach/2413/00331407/R2_koufuzei.pdf
osaka2020df = df.loc[(df['year'] == 2020) & (df['city']=='大阪市')]
osaka2020dfrough =  df_rough.loc[(df_rough['year'] == 2020) & (df_rough['city']=='大阪市')]
assert(osaka2020df['demand'].values == 654898101*1000)
assert(osaka2020df['income'].values == 621727850*1000)
## do i have the sakugo column?
sakai2020df = df.loc[(df['year'] == 2020) & (df['city']=='堺市')]
sakai2020dfrough =  df_rough.loc[(df_rough['year'] == 2020) & (df_rough['city']=='堺市')]
assert(sakai2020df['demand'].values == 169411859*1000)
assert(sakai2020df['income'].values == 136809228*1000)
## I'm missing the 2 sakugo columns!! important, at least to get all cities to agree on the adjustment factor. otherwise can just get the most common number as the adjustment factor (since this represents sakugo 0 0)
## I think I need to call them for the sakugo info... Think about whether I really need it. It seems smallish? Estimate size of effect.

# df_all = df_income.merge(df.drop_duplicates(), on=['year','code', 'prefecture', 'city','deficit-or-surplus'], 
#                    how='left', indicator=True)

# df_all[df_all['_merge'] == 'left_only']

# df_income.loc[(df_income['year']==2020) & (df_income['code']=='C282219110000')]

# df_income.loc[(df_income['city']=='丹波篠山市')]

# df_demand.loc[(df_demand['year']==2020) & (df_demand['code']=='C282219110000')]

# df_demand.loc[df_demand['city']=='篠山市']
# df_income.loc[df_income['city']=='篠山市']

assert(len(df) == len(df_income))

# df_income.loc[df_income['year'].(df['city'] & ~df_income['year'].isin(df['city'])]


#     DONATIONS_ROUGH_FILE = os.path.join(FN_DATA_FOLDER, 'donations-rough/total_gain_backup.xlsx')
#     years = ['H'+str(i) for i in range(20,31)] + ['R1','R2']
#     western_years = list(range(2008, 2021))

#     DONATIONS_ROUGH_FILE = os.path.join(FN_DATA_FOLDER, 'donations-rough/total_gain.xlsx')
#     years = ['H'+str(i) for i in range(20,31)] + ['R1','R2', 'R3']
#     western_years = list(range(2008, 2022))

#     colnames = ['prefecture', 'city']
#     for year in western_years:
#         colnames.append(str(year)+'-donations') #units of this column is thousands of yen (for now)
#         colnames.append(str(year)+'-donations-count')

#     df = pd.read_excel(DONATIONS_ROUGH_FILE, header=3,names=colnames)

#     for i in range(len(df["prefecture"])):
#         if df["prefecture"][i] == '市町村合計':
#             df.at[i,"prefecture"] = df["prefecture"][i-1]
#             df.at[i,"city"] = 'prefecture_cities_total'
#         elif df["prefecture"][i] == '合計':
#             df.at[i,"prefecture"] = df["prefecture"][i-1]
#             df.at[i,"city"] = 'prefecture_all_total'
#         elif df["prefecture"][i] == '全国合計':
#             df.at[i,"prefecture"] = 'japan'
#             df.at[i,"city"] = 'total'
#         if pd.isna(df["city"][i]):
#             df.at[i,"city"] = 'prefecture'
    
#     for year in western_years:
#         df[str(year)+'-donations'] = df[str(year)+'-donations'] * 1000 #units of this column is now yen
    
#     prefecture_list = df["prefecture"].unique().tolist()
#     prefecture_list.remove('japan')

#     for i in range(len(prefecture_list)):
#         prefecture = prefecture_list[i]
#         rows = df.loc[df['prefecture'] == prefecture]
#         data = rows.iloc[1:-2,2:]
#         computed_cities_subtotal = data.sum(axis=0) 
#         written_cities_subtotal = rows.iloc[-2, 2:]
#         assert np.abs((computed_cities_subtotal-written_cities_subtotal).sum()) < 5000
#         computed_subtotal = computed_cities_subtotal + rows.iloc[0,2:]
#         written_subtotal = rows.iloc[-1, 2:]
#         assert np.abs((computed_subtotal-written_subtotal).sum()) < 5000
#         if i == 0:
#             total = computed_subtotal
#         else:
#             total += computed_subtotal

#     written_total = df.loc[df['prefecture'] == 'japan'].iloc[0,2:]
#     assert (np.abs(written_total-total)<5000).all()

#     df = df.loc[(df['city'] != 'total') & (df['city'] != 'prefecture_all_total') & (df['city'] != 'prefecture_cities_total')]

#     df['prefecturecity'] = df["prefecture"]+df["city"]
    
#     df.reset_index(drop=True, inplace=True)

#     return df

# def load_donations_by_year(year, correct_errors=True):

#     cols = {'code6digit':str, 'prefecture':str, 'city':str, 'donations-count':np.int64, 'donations':np.int64, 'donations-from-outside-count':np.int64, 'donations-from-outside':np.int64, 'donations-disaster-count':np.int64, 'donations-disaster':np.int64, 'product-cost':np.int64, 'shipping-cost':np.int64, 'total-cost':np.int64}

#     forced_coltypes = {'code6digit':str, 'prefecture':str}

#     if year == 'R3':
#         columnindices = [0,1,2,3,4,5,6,9,10,11,12,17]
#         skiprows=12
#         ncols=97
#     if year == 'R2':
#         columnindices = [0,1,2,3,4,5,6,9,10,11,12,17]
#         skiprows=13
#         ncols=96
#     if year == 'R1':
#         columnindices = [0,1,2,3,4,5,6,9,10,11,12,17]
#         skiprows=16
#         ncols=114
#     if year == 'H30':
#         columnindices = [0,1,2,3,4,5,6,9,10,11,12,17]
#         skiprows=5
#         ncols=114
#     if year == 'H29':
#         columnindices = [0,1,2,3,4,6,7,12,13,24,25,30]
#         skiprows=5
#         ncols=118
#     if year == 'H28':
#         columnindices = [0,1,2,3,4,6,7,12,13,24,25,30]
#         skiprows=6
#         ncols=127

#     df = pd.read_excel(DONATIONS_FOLDER+year+'_gain.xlsx', skiprows=skiprows,header=None,usecols=columnindices, names=cols.keys(),dtype=forced_coltypes)
#     df.loc[(df.city==0) | pd.isna(df.city) | (df.city=='-'),"city"] = "prefecture"

#     if (year == 'H29') or (year == 'H28'):
#         df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"
#         df.loc[(df.city=='那珂川町') & (df.prefecture=='福岡県') ,'city'] = '那珂川市'
    
#     df.loc[(df.prefecture=='鹿児島'),'prefecture'] = "鹿児島県"
#     df.loc[(df.prefecture=='岡山'),'prefecture'] = '岡山県'

#     df['prefecturecity']=df["prefecture"]+df["city"]
#     df['donations-from-outside']=pd.to_numeric(df['donations-from-outside'], errors ='coerce').fillna(0).astype('int')
#     df['donations-from-outside-count']=pd.to_numeric(df['donations-from-outside-count'], errors ='coerce').fillna(0).astype('int')
#     df['product-cost']=pd.to_numeric(df['product-cost'], errors ='coerce').fillna(0).astype('int')
#     df['shipping-cost']=pd.to_numeric(df['shipping-cost'], errors ='coerce').fillna(0).astype('int')
#     df['total-cost']=pd.to_numeric(df['total-cost'], errors ='coerce').fillna(0).astype('int')
#     df['donations-disaster']=pd.to_numeric(df['donations-disaster'], errors ='coerce').fillna(0).astype('int')
#     df['donations-disaster-count']=pd.to_numeric(df['donations-disaster-count'], errors ='coerce').fillna(0).astype('int')

#     df = df.astype(cols)

#     if correct_errors:
#         if (year == 'H28'):
#             ## Fat finger errors here led to outrageously large donations-from-outside values
#             df.loc[df['prefecturecity']=='北海道芽室町','donations-from-outside'] =  df.loc[df['prefecturecity']=='北海道芽室町','donations'] 
#             df.loc[df['prefecturecity']=='山形県朝日町','donations-from-outside'] =  df.loc[df['prefecturecity']=='山形県朝日町','donations'] 
#             df.loc[df['prefecturecity']=='東京都神津島村','donations-from-outside'] =  df.loc[df['prefecturecity']=='東京都神津島村','donations'] 
#             df.loc[df['prefecturecity']=='山梨県北杜市','donations-from-outside'] = 13227000
#             ## Here it looks like within and without got swapped
#             df.loc[df['prefecturecity']=='栃木県那珂川町',['donations-count','donations', 'donations-from-outside-count', 'donations-from-outside']] = df.loc[df['prefecturecity']=='栃木県那珂川町',['donations-from-outside-count','donations-from-outside', 'donations-count', 'donations']].values
#             ## Fat finger error leads to outrageously small gain value
#             df.loc[df['prefecturecity']=='埼玉県上里町','donations'] =  df.loc[df['prefecturecity']=='埼玉県上里町','donations']*10 #1650000
#             df.loc[df['prefecturecity']=='静岡県御殿場市','donations'] =  df.loc[df['prefecturecity']=='静岡県御殿場市','donations']*10 
#             ## Minor typo 
#             df.loc[df['prefecturecity']=='山梨県prefecture','donations'] =  24151001
#             df.loc[df['prefecturecity']=='鹿児島県薩摩川内市','donations'] =  df.loc[df['prefecturecity']=='鹿児島県薩摩川内市','donations-from-outside']
#         if year == 'H29':
#             pass
#         if year == 'H30':
#             ## Fat finger errors here led to major error in donations-from-outside
#             df.loc[df['prefecturecity']=='宮崎県諸塚村','donations-from-outside'] =  df.loc[df['prefecturecity']=='宮崎県諸塚村','donations'] 
#             ## Fat finger errors here led to minor errors in donations-from-outside
#             df.loc[df['prefecturecity']=='北海道羅臼町','donations-from-outside'] =  df.loc[df['prefecturecity']=='北海道羅臼町','donations'] 
#             df.loc[df['prefecturecity']=='静岡県湖西市','donations-from-outside'] =  df.loc[df['prefecturecity']=='静岡県湖西市','donations'] 
#             df.loc[df['prefecturecity']=='熊本県八代市','donations-from-outside'] =  df.loc[df['prefecturecity']=='熊本県八代市','donations'] 
#             df.loc[df['prefecturecity']=='鹿児島県湧水町','donations-from-outside'] =  df.loc[df['prefecturecity']=='鹿児島県湧水町','donations'] 
#         if year == 'R1':
#             ## Minor issue 
#             df.loc[df['prefecturecity']=='福井県美浜町','donations'] =  df.loc[df['prefecturecity']=='福井県美浜町','donations-from-outside'] #1650000

#     df['net-gain'] = df['donations']-df['total-cost']

#     df['code'] = df['code6digit'].apply(lambda s: s if pd.isna(s) else s[:-1])
#     df.drop(['code6digit'], inplace=True, axis=1)

#     if correct_errors:
#         assert( len(df.loc[(df["donations-from-outside"]-1 > df["donations"])]) == 0)
#     # problematic_rows = df.loc[df["donations-from-outside"]-1 > df["donations"]].index
#     # print(df.loc[problematic_rows, ["prefecturecity", "donations", "donations-count", "donations-from-outside", "donations-from-outside-count"]])
#     # print(df.loc[problematic_rows, "donations"]/ df.loc[problematic_rows, "donations-count"])
    
#     #print(df.loc[(df["donations-from-outside"] <  df["donations"]/6) & (df["donations-from-outside"]>0)])

#     # print(df.loc[(df["donations"]/df["donations-count"]) > 10**6])
#     # print(df.loc[(df["donations"]/df["total-cost"]) > 100])

#     ## Flagging unfixable errors

#     df['flag'] = False
#     df.loc[(df["product-cost"] + df["shipping-cost"]) > df["donations"], 'flag'] = True
#     #print(df.loc[(df["product-cost"] + df["shipping-cost"]) > df["donations"]])


#     # print(df['donations-from-outside'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index]/df['donations-count'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index])
#     # print(df['donations'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index]/df['donations-count'].iloc[df.loc[(df["donations-from-outside"]-1 > df["donations"])].index])

#     # plt.plot(np.sort(df["donations"] / df["donations-count"]))
#     # plt.yscale('log')
#     # plt.show()

#     return df

# def load_deductions_by_year(year):
#     cols = {'prefecture':object, 'city':object, 
#         'city-reported-people':np.int64, 'city-reported-donations':np.int64, 'city-reported-deductions':np.int64, 
#         'pref-reported-people':np.int64, 'pref-reported-donations':np.int64, 'pref-reported-deductions':np.int64}

#     if year == 'R4':
#         columnindices = [0,1,53,54,55,56,57,58]
#         skiprows=18
#         sheetnumber=0
#         ncols=59
#     if year == 'R3':
#         columnindices = [0,1,53,54,55,56,57,58]
#         skiprows=18
#         sheetnumber=0
#         ncols=59
#     if year == 'R2':
#         columnindices = [0,1,53,54,55,56,57,58]
#         skiprows=18
#         sheetnumber=1
#         ncols=59
#     if year == 'R1':
#         columnindices = [0,1,53,54,55,56,57,58]
#         skiprows=18
#         sheetnumber=0
#         ncols=59
#     if year == 'H30':
#         columnindices = [0,1,52,53,54,55,56,57]
#         skiprows=19
#         sheetnumber=0
#         ncols=58
#     if year == 'H29':
#         columnindices = [0,1,53,54,55,56,57,58]
#         skiprows=17
#         sheetnumber=0
#         ncols=59
#     if year == 'H28':
#         columnindices = [0,1,53,54,55,56,57,58]
#         skiprows=15
#         sheetnumber=0
#         ncols=59

#     df = pd.read_excel(DEDUCTIONS_FOLDER+year+'_loss.xlsx', skiprows=skiprows,sheet_name=sheetnumber,header=None,usecols=columnindices, names=cols.keys())

#     for i in range(len(df["prefecture"])):
#         if df["prefecture"][i][-2:] == '集計' or df["prefecture"][i] == '合計':
#             df.at[i,"prefecture"] = df["prefecture"][i-1]
#             df.at[i,"city"] = 'prefecture_cities_total'
#         if df["prefecture"][i] == '総計' or df["prefecture"][i] =='全国合計':
#             df.at[i,"prefecture"] = 'japan'
#             df.at[i,"city"] = 'total'

#     df.loc[(df.prefecture=='岡山'),'prefecture'] = '岡山県'
#     df.loc[(df.prefecture=='沖縄'),'prefecture'] = "沖縄県"
#     df.loc[(df.prefecture=='青森'),'prefecture'] = "青森県"
#     df.loc[(df.city=='篠山市'),'city'] = "丹波篠山市"
#     df.loc[(df.city=='那珂川町') & (df.prefecture=='福岡県') ,'city'] = '那珂川市'
#     df.loc[(df.prefecture=='鹿児島'),'prefecture'] = "鹿児島県"
#     df.loc[(df.city=='宝塚市'),'city'] = '宝塚市'
#     df.loc[(df.city=='富谷町'),'city'] = '富谷市'
    
#     prefecture_list = df["prefecture"].unique().tolist()
#     prefecture_list.remove('japan')

#     for i in range(len(prefecture_list)):
#         prefecture = prefecture_list[i]
#         rows = df.loc[df['prefecture'] == prefecture]
#         data = rows.iloc[0:-1,2:]
#         computed_subtotal = data.sum(axis=0) 
#         written_subtotal = rows.iloc[-1, 2:]
#         assert np.abs((computed_subtotal-written_subtotal).sum()) < 1
#         if i == 0:
#             total = computed_subtotal
#         else:
#             total += computed_subtotal

#     written_total = df.loc[df['prefecture'] == 'japan'].iloc[0,2:]
#     assert (np.abs(written_total-total)<100).all()

#     df = df.astype(cols)

#     df['prefecturecity'] = df["prefecture"]+df["city"]
#     df['reported-people'] = df[['city-reported-people','pref-reported-people']].max(axis=1)
#     df['reported-donations'] = df[['city-reported-donations','pref-reported-donations']].max(axis=1)
#     df['deductions'] = df['city-reported-deductions']+df['pref-reported-deductions']
    
#     df = df.loc[(df['city'] != 'total') & (df['city'] != 'prefecture_cities_total')]
#     df.reset_index(drop=True, inplace=True)
    
#     df['prefecturecity'] = df["prefecture"]+df["city"]

#     return df

# def combine_loss_gain(df_loss, df_gain):
    
#     df = df_gain.copy()

#     new_columns = []
#     for column in df_loss.columns:
#         if column not in df_gain.columns:
#             new_columns.append(column)
#             df[column] = np.nan

#     df['netgainminusdeductions'] = np.nan

#     loss_index = 0 
#     for i in range(len(df_gain)):

#         if df_gain['city'][i]!='prefecture':
#             try:
#                 loss_index = df_loss[df_loss['prefecturecity'] == df_gain['prefecturecity'][i]].index[0]
#                 for column in new_columns:
#                     df.loc[i, column] = df_loss[column][loss_index]
#                     df.loc[i,'netgainminusdeductions']= df_gain['net-gain'][i]-df_loss['deductions'][loss_index]
                
#             except IndexError():
#                 print("couldn't find city")

#     df = df.set_index('prefecturecity')
    
#     return df


# def clean_data(correct_errors=True):
    
#     print('loading rough donations table')
#     df_rough = load_donations_rough()

#     years = np.array([int(i) for i in [2016,2017,2018,2019,2020,2021]])
#     year_labels = ['H28','H29','H30','R1','R2','R3']
#     corresponding_loss_labels = ['H29','H30','R1','R2','R3','R4']

#     year_df_list = []
#     for i,year in enumerate(year_labels):
#         print('loading gain ', year)
#         df_gain = load_donations_by_year(year,correct_errors=correct_errors)
#         print('loading loss ', corresponding_loss_labels[i])
#         df_loss = load_deductions_by_year(corresponding_loss_labels[i])
#         print('running consistency checks ', year)
#         assert ((df_gain["prefecturecity"] == df_rough["prefecturecity"]).all())
#         if not correct_errors:
#             assert (np.abs(df_gain["donations"] - df_rough[str(years[i])+"-donations"]) < 1000).all()

#         print('merging loss and gain dfs')
#         year_df = combine_loss_gain(df_loss, df_gain)
#         year_df_list.append(year_df)
    
#     full_data_array = xr.concat([year_df.to_xarray() for year_df in year_df_list], dim=xr.DataArray(years,dims='year'))

#     ## Now we also massage the rough array into an easier to handle form
#     western_years = list(range(2008, 2022))
#     df_rough['code']=df_gain['code'] ## This assumes that the municipalities are listed in the same order in the two files

#     df_donations = df_rough.drop([str(year) +'-donations-count' for year in western_years],axis=1)
#     df_donations.columns = df_donations.columns.str.replace('-donations', '')
#     df_donations =  pd.melt(df_donations, id_vars=['prefecture', 'city','prefecturecity','code'], value_vars=[str(year) for year in western_years],var_name='year', value_name='donations')

#     df_count = df_rough.drop([str(year) +'-donations' for year in western_years],axis=1)
#     df_count.columns = df_count.columns.str.replace('-donations-count', '')
#     df_count =  pd.melt(df_count, id_vars=['prefecture', 'city','prefecturecity','code'], value_vars=[str(year) for year in western_years],var_name='year', value_name='donations-count')

#     df_rough_melted = df_donations.merge(df_count)
#     df_rough_melted['year'] = pd.to_numeric(df_rough_melted['year']).astype('int')

#     return full_data_array, df_rough_melted

# try:
#     furusato_arr = pd.read_parquet(CACHED_FILE).to_xarray()
#     furusato_rough_df = pd.read_parquet(ROUGH_CACHED_FILE)
# except FileNotFoundError:
#     if not checkfordata():
#         getdata()
#     furusato_arr, furusato_rough_df = clean_data()
#     furusato_arr.to_dataframe().to_parquet(CACHED_FILE)
#     furusato_rough_df.to_parquet(ROUGH_CACHED_FILE)

# furusato_df = furusato_arr.to_dataframe().reset_index().fillna(value=np.nan)
# furusato_df = furusato_df.drop(furusato_df.loc[pd.isna(furusato_df['donations'])].index)

# furusato_pref_df = furusato_df.groupby(['prefecture','year']).sum().reset_index()
# ## TODO: Drop or update the fields that don't just sum in the pref_df

# furusato_sum_df = furusato_df.groupby(['code','prefecturecity','prefecture', 'city']).sum().reset_index().drop('year', axis=1)
# furusato_pref_sum_df = furusato_pref_df.groupby(['prefecture']).sum().reset_index().drop('year', axis=1)
# ## TODO: same here