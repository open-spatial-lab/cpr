# cpr
Californians for Pesticide Reform: Data Collaboratory 2023


## Data Biography
- PUR data `data/pur/` is collected from 2001 to 2021
- Section data `data/sections` is a 2023 vintage of the PUR sections. `sections.parquet` combining all county data
- `data/geo/cb_2021_06_tract_500k.zip` contains 2021 census tracts for california
- `data/geo/zip-county-crosswalk.xlsx` uses Q3 2023 HUD zip to county crosswalk (https://www.huduser.gov/apps/public/uspscrosswalk/home)
- `data/geo/cb_2020_us_zcta520_500k (1).zip` contains 2020 ZCTA boundaries, which are the most recent see https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2021.html#list-tab-1883739534
- Community & demographic data `data/community vars/` are from [American Community Survey 2021 (5-Year Estimates)](https://www.socialexplorer.com/data/ACS2021_5yr/metadata/), accessed via Social Explorer. Variables include: A04001 Hispanic of Latino by Race, A12002 Highest Educational Attainment for Population 25 Years and Over, A14006 Median Household Income, and A17004 Industry by Occupation for Employed Population 16 Years and Over. 

## Scripts
- `scripts/download_pur.py` helps to download and parse PUR data from 2001 to 2021
- `scripts/download_sections.py` helps to download GIS data
