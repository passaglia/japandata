# japandata

[![PyPI](https://img.shields.io/pypi/v/japandata?label=latest%20release)](https://pypi.org/project/japandata/)

**japandata** is a python library that provides easy access to geographic data about Japan:

* [`japandata.maps`](#maps): Maps of Japan, its prefectures, and municipalities,
* [`japandata.population`](#population): Demographic information,
* [`japandata.readings`](#readings): Kana and romaji readings of place names,
* [`japandata.indices`](#indices): Municipal economic health indicators.

<!-- TODO: Add a nice plot here  -->

## japandata.maps 

`japandata.maps` provides national, prefectural, and municipal topojson maps from 1920 to today.

```python
from japandata.maps import load_map

prefecture_map = load_map(date=2022, scale='jp_pref', quality='coarse')
```

See `notebooks/maps.ipynb` to understand the different types of maps that can be loaded.

Source: [Asanobu Kitamoto, ROIS-DS Center for Open Data in the Humanities](https://geoshape.ex.nii.ac.jp/city/choropleth/)
License: CC BY-SA 4.0.


## japandata.population

`japandata.population` provides national, prefectural, and municipal demographic data annually from 1967.

```
from japandata.population.data import japan_pop, pref_pop, city_pop,
                                      japan_age, pref_age, city_age
```

* `japan_pop`, `pref_pop`, `city_pop`: Contain total population, # of men and women, # of households, # of births, # deaths, and # migrations, for Japanese and non-Japanese residents.
* `japan_age`, `pref_age`, `city_age`: Contain age distributions split by gender for Japanese and non-Japanese residents.

See `notebooks/population.ipynb` for example uses of this dataset.

Source: [Basic Register of Residents (住民基本台帳)](https://www.soumu.go.jp/main_sosiki/jichi_gyousei/daityo/gaiyou.html) via [Official Statistics Portal Site](https://www.e-stat.go.jp/stat-search/files?page=1&toukei=00200241&tstat=000001039591).
License: [CC BY 4.0 International](https://www.soumu.go.jp/menu_kyotsuu/policy/tyosaku.html#tyosakuken).

<!-- #### TODO

-- Show how to compute fertility rate

-- Show how to compute working population: 15-64

-->

## japandata.indices 

`japandata.indices.data` provides fiscal health indices for municipal and prefectural governments annually from 2005. These indices are used to e.g. determine fiscal transfers between municipalities.

```python
from japandata.indices import city, pref, prefmean
```

`city` covers municipal governments, `pref` covers prefectural governments, and `prefmean` provides weighted means of municipal indices grouped by prefecture.

See `notebooks/indices.ipynb` for example uses of this dataset.

Source: [Ministry of Internal Affairs](https://www.soumu.go.jp/iken/shihyo_ichiran.html).
License: [CC BY 4.0 International](https://www.soumu.go.jp/menu_kyotsuu/policy/tyosaku.html#tyosakuken).

<!-- 
The `economic-strength-index` (財政力指数) shows the economic strength of a local government. It is the ratio of the standardized tax receipts (基準財政収入額 -- tax receipts times 0.75) to the standardized economic burden (基準財政需要額 -- an estimated cost required to provide government services), averaged over the past three years. If the economic strength index is greater than 1, the local government has an economic surplus and will transfer funds (地方交付税) to local governments with an economic strength index less than one.

For the 23 special wards of Tokyo, which all pay transfer taxes to the rest of Japan, the value in this column is instead a different figure which is used to determine internal financial transfers between the wards (特別区財政調整交付金).

The `regular-expense-rate` (経常収支比率) is an index which shows the economic flexibility of a local government. It is the ratio of general expenses (such as personnel expenses, welfare expenses, and debt repayments) to general, non-specified income (local tax income, income from tax transfered between municipalities, and debt issuances), expressed as percentage. The higher this percentage, the less financial wiggle room a municipality has.

The `debt-service-rate` (実質公債費比率) is the ratio of the annual cost of debt servicing (repaying principal and interest) to the general non-specified income of a municality (more precisely the 標準財政規模), averaged over the preceding three years and expressed as a percentage. Municipalities face increasing restrictions on debt issuances when this ratio exceeds 18%, 25%, and 35%.

Available from 2008 (H20) and onwards, the `future-burden-rate` (将来負担比率) is the ratio of the total future liabilities (such as debt) to the annual income (標準財政規模) expressed as a percentage. A law indicates municipalities should remain below 350% and prefectures and designated cities below 400%.

Prior to 2008, the `debt-restriction-rate` (起債制限比率) was used to regulate municipal debt issuances. It is similar to the `debt-service-rate` but computed slightly differently.

The `laspeyres` index here measures the salary of municipal government employees relative to national government employees, controlling for educational history and seniority. A figure greater than 100 indicates municipal employees are being paid more than national employees. -->



### japandata.readings

`japandata.readings` provides kana and romaji pronunciation information for Japanese place names.


```python
from japandata.readings import city_names, pref_names
```

See `notebooks/readings.ipynb` for code to integrate this information with the maps.


# Installation

``` 
pip install japandata
```

# License
Code: MIT
Data: Noted above.
