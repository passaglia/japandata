# JapanData

[![PyPI](https://img.shields.io/pypi/v/japandata?label=latest%20release)](https://pypi.org/project/japandata/)
[![PyPI - License](https://img.shields.io/pypi/l/japandata)](https://github.com/passaglia/japandata/blob/main/LICENSE.md)

**JapanData** is a python package which provides access to datasets about Japan. It includes:

* [`japandata.maps`](#maps): Geographic information,
* [`japandata.population`](#population): Population statistics,
* [`japandata.indices`](#indices) : Municipal economic health indicators.
* [`japandata.readings`](#readings) : Kana and romaji readings of the names of japanese municipalities and prefectures

Jupyter notebooks in the `/examples` folder demonstrate how to use these datasets.

This package is provided under a MPL 2.0 license. Each dataset is subject to its own license noted below. The datasets are fetched from the companion repository [**JapanData-sources**](https://github.com/passaglia/japandata-sources) when needed.

## Installation

**JapanData** can be installed using pip

``` 
pip install japandata
```

If you wish to enhance or extend **JapanData**, you can make changes by cloning this repository and then either adding `src/japandata` directly to your python path or by installing the local version using pip

```
python3 -m build
pip install -e .
```

## Available Datasets

### Maps 

`japandata.maps.data` provides maps of Japan, its prefectures, and its municipalities, from 1920 to today. These maps are sourced from [Asanobu Kitamoto, ROIS-DS Center for Open Data in the Humanities](https://geoshape.ex.nii.ac.jp/city/choropleth/), and they are licensed CC BY-SA 4.0.  They take the form of geopandas dataframes containing topojson maps.

```
from japandata.maps.data import load_map

map_df = load_map(date, level, quality)
```

`date` should be a date (e.g. `2015-04-31`) or a year (`2015`). Maps are available for a range of dates starting in 1920, and this function will return the most recent map available on or before `date` . Use the `japandata.maps.data.get_dates()` function to check the available dates.

`level` should be  `prefecture`, `local`, `local_dc`, or `japan`. `prefecture` yields a geopandas dataframe of Japan's prefectures, `local` a dataframe of its localities, `local_dc` a dataframe in which the localities making up [*designated cities*](https://en.wikipedia.org/wiki/Cities_designated_by_government_ordinance_of_Japan) are merged, and `japan` a dataframe containing a single geometry object of the whole of japan.

`quality` should be one of `coarse`, `low`, `medium`, `high` and controls the geometrical detail of the map. For many purposes `coarse` is sufficient.

<!-- TODO: Update docs  -->
### Population

`japandata.population.data` provides data about the population and demographics of japan, at the national, prefectural, and municipal level, annually from 1967 to 2020. This information is sourced from the [Basic Register of Residents (住民基本台帳)](https://www.soumu.go.jp/main_sosiki/jichi_gyousei/daityo/gaiyou.html) via the [Official Statistics Portal Site](https://www.e-stat.go.jp/stat-search/files?page=1&toukei=00200241&tstat=000001039591) and is licensed [CC BY 4.0 International](https://www.soumu.go.jp/menu_kyotsuu/policy/tyosaku.html#tyosakuken).


```
from japandata.population.data import japan_pop_df, pref_pop_df, local_pop_df
```

* `japan_pop_df`: Pandas dataframe with information about Japan, 1967-2020
* `pref_pop_df`: Information about each prefecture, 1967-2020
* `local_pop_df`: Information about each locality, 1995-2020. Contains redundancies: both designated cities and their constituent subdivisions are included.

The data gradually becomes more detailed as time goes on, with early data containing only the total population, the gender breakdown, and the number of households, while later data includes e.g. the number of births and deaths. Each year is a Japanese fiscal year, stretching from April 1st to March 31st of the subsequent calendar year. For example, the row marked '1995' contains the number of births from April 1st, 1995 to March 31st, 1996. The total population in the '1995' row is the population at the end of this period, on March 31st 1996.

<!-- #### TODO

-- update docs for pop

-- refactor and add docs for age data

-- Is there a way to compute fertility rate? 

-- Working population: 15-64

-->

### Indices 

`japandata.indices.data` contains indices of the economic health of each municipality and prefecture in Japan. These indices are produced by the government for various purposes, such as to determine financial transfers between municipalities or to restrict municipal debt issuances. The data is provided by the [Ministry of Internal Affairs](https://www.soumu.go.jp/iken/shihyo_ichiran.html) and is licensed [CC BY 4.0 International](https://www.soumu.go.jp/menu_kyotsuu/policy/tyosaku.html#tyosakuken). It covers FY2005 to FY2020.

```
from japandata.indices.data import local_ind_df,  designatedcity_ind_df, capital_ind_df, pref_ind_df, prefmean_ind_df
```

`local_ind_df` is a dataframe containing the economic health indices for each local government of japan, with a separate row for each year and municipality. `designatedcity_ind_df` contains the indices for just the designated cities and `capital_ind_df` for just the prefectural capitals. `pref_ind_df` contains indices computed for each prefecture, while `prefmean_ind_df` contains the average of the indices for the local municipal indices within each prefecture.

A detailed explanation of each index is available in Japanese from the official data source above. Here is a rough summary in English.

The `economic-strength-index` (財政力指数) shows the economic strength of a local government. It is the ratio of the standardized tax receipts (基準財政収入額 -- tax receipts times 0.75) to the standardized economic burden (基準財政需要額 -- an estimated cost required to provide government services), averaged over the past three years. If the economic strength index is greater than 1, the local government has an economic surplus and will transfer funds (地方交付税) to local governments with an economic strength index less than one.

For the 23 special wards of Tokyo, which all pay transfer taxes to the rest of Japan, the value in this column is instead a different figure which is used to determine internal financial transfers between the wards (特別区財政調整交付金).

The `regular-expense-rate` (経常収支比率) is an index which shows the economic flexibility of a local government. It is the ratio of general expenses (such as personnel expenses, welfare expenses, and debt repayments) to general, non-specified income (local tax income, income from tax transfered between municipalities, and debt issuances), expressed as percentage. The higher this percentage, the less financial wiggle room a municipality has.

The `debt-service-rate` (実質公債費比率) is the ratio of the annual cost of debt servicing (repaying principal and interest) to the general non-specified income of a municality (more precisely the 標準財政規模), averaged over the preceding three years and expressed as a percentage. Municipalities face increasing restrictions on debt issuances when this ratio exceeds 18%, 25%, and 35%.

Available from 2008 (H20) and onwards, the `future-burden-rate` (将来負担比率) is the ratio of the total future liabilities (such as debt) to the annual income (標準財政規模) expressed as a percentage. A law indicates municipalities should remain below 350% and prefectures and designated cities below 400%.

Prior to 2008, the `debt-restriction-rate` (起債制限比率) was used to regulate municipal debt issuances. It is similar to the `debt-service-rate` but computed slightly differently.

The `laspeyres` index here measures the salary of municipal government employees relative to national government employees, controlling for educational history and seniority. A figure greater than 100 indicates municipal employees are being paid more than national employees.

### Readings

```
from japandata.readings.data import names_df, pref_names_df 
```

These dataframes contain kanji, kana, and romaji readings of the names of Japanese municipalities and prefectures.

<!-- ### GENERAL TODO
-- Add census data?  https://www.stat.go.jp/data/kokusei/2020/kekka.html

-- Labour force survey: https://www.e-stat.go.jp/stat-search/files?page=1&toukei=00450071&tstat=000001011791

-- Gender balance info: Government https://www.gender.go.jp/policy/suishin_law/csv_dl/index.html Private: https://positive-ryouritsu.mhlw.go.jp/positivedb/

-- weather: http://tenkiapi.jp/

-->
