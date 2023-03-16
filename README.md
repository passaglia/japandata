# japandata

[![PyPI](https://img.shields.io/pypi/v/japandata?label=latest%20release)](https://pypi.org/project/japandata/)

**japandata** is a python library that provides easy access to detailed geographic data about Japan.

* [`japandata.maps`](#maps): Maps of Japan, its prefectures, and municipalities
* [`japandata.population`](#population): Demographic data
* [`japandata.indices`](#indices): Economic health indicators
* [`japandata.readings`](#readings): Kana and romaji readings of place names

<!-- TODO: Add a nice plot here  -->

## japandata.maps 

`japandata.maps` provides national, prefectural, and municipal topojson maps from 1920 to today.

```python
from japandata.maps import load_map

prefecture_map = load_map(date=2022, scale='jp_pref', quality='coarse')
```

See `notebooks/maps.ipynb` to understand the different types of maps that can be loaded.

- Source: [Asanobu Kitamoto, ROIS-DS Center for Open Data in the Humanities](https://geoshape.ex.nii.ac.jp/city/choropleth/)
- License: CC BY-SA 4.0


## japandata.population

`japandata.population` provides national, prefectural, and municipal demographic data annually from 1967.

```python
from japandata.population.data import japan_pop, pref_pop, city_pop,
                                      japan_age, pref_age, city_age
```

* `japan_pop`, `pref_pop`, `city_pop`: Contain data on total population, gender split, number of households, births, deaths, and migrations, for Japanese and non-Japanese residents.
* `japan_age`, `pref_age`, `city_age`: Contain age distributions split by gender for Japanese and non-Japanese residents.

See `notebooks/population.ipynb` for example uses of this dataset.

- Source: [Basic Register of Residents (住民基本台帳)](https://www.soumu.go.jp/main_sosiki/jichi_gyousei/daityo/gaiyou.html) via [Official Statistics Portal Site](https://www.e-stat.go.jp/stat-search/files?page=1&toukei=00200241&tstat=000001039591)
- License: [CC BY 4.0 International](https://www.soumu.go.jp/menu_kyotsuu/policy/tyosaku.html#tyosakuken)

## japandata.indices

`japandata.indices.data` provides fiscal health indices for municipal and prefectural governments annually from 2005. These indices are used to e.g. determine fiscal transfers between municipalities.

```python
from japandata.indices import city, pref, prefmean
```

`city` covers municipal governments, `pref` covers prefectural governments, and `prefmean` provides weighted means of municipal indices grouped by prefecture.

See `notebooks/indices.ipynb` for example uses of this dataset.

- Source: [Ministry of Internal Affairs](https://www.soumu.go.jp/iken/shihyo_ichiran.html)
- License: [CC BY 4.0 International](https://www.soumu.go.jp/menu_kyotsuu/policy/tyosaku.html#tyosakuken)

### japandata.readings

`japandata.readings` provides kana and romaji pronunciation information for Japanese place names.


```python
from japandata.readings import city_names, pref_names
```

See `notebooks/readings.ipynb` for code to integrate this information with the maps.


# Installation

```bash
$ pip install japandata
```

# Licenses

- Code: MIT
- Data: Noted above
