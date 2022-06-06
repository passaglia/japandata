'''
readings/data.py

Module which provides readings for some place names

Author: Sam Passaglia
'''

import pandas as pd
import numpy as np
import xarray as xr
import os
import romkan
import jaconv


DATA_URL = "https://github.com/passaglia/japandata-sources/raw/main/readings/readingsdata.tar.gz"
DATA_FOLDER = os.path.join(os.path.dirname(__file__),'readingsdata/')

READINGS_FILE = os.path.join(DATA_FOLDER, 'R2_loss.xlsx')

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

def load_readings_R2file():

    colnames = ['code6digit','prefecture', 'city', 'prefecture-kana', 'city-kana']

    df = pd.read_excel(READINGS_FILE,names=colnames,dtype={'code6digit':str})
    df['code'] = df['code6digit'].apply(lambda s: s if pd.isna(s) else s[:-1])
    df.drop(['code6digit'], inplace=True, axis=1)

    prefecture_df = df.loc[pd.isna(df['city'])].drop(['city','city-kana'],axis=1).reset_index(drop=True)
    prefecture_df['code'] = prefecture_df['code'].apply(lambda s: s[0:2])
    prefecture_df['prefecture-reading'] = prefecture_df['prefecture-kana'].apply(lambda s: romkan.to_roma(jaconv.h2z(s).strip('ケン'))).str.replace('osakafu', 'osaka').str.replace('toukyouto', 'toukyou').str.replace('kyoutofu', 'kyouto')

    df = df.loc[~pd.isna(df['city'])].reset_index(drop=True)

    return df, prefecture_df

names_df, pref_names_df = load_readings_R2file()
