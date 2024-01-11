# Pesticide Data Explorer: Californians for Pesticide Reform

## About

### OSL Data Collaboratory
This project is part of the Open Spatial Lab's 2023 Data Collaboratory. The Collaboratory is a 6-month program where OSL engages with social impact organizations to build a customized tool for data management, analysis, communication, and visualization. Circulate San Diego’s organizational engagement and feedback directly informs this work. 

Based at the University of Chicago Data Science Institute, the Open Spatial Lab creates open source data tools and analytics to solve problems using geospatial data science. Read more about OSL at https://datascience.uchicago.edu/research/open-spatial-lab/. 

### Project Scope
**About**: Californians for Pesticide Reform (CPR) is a statewide coalition of more than 190 organizations that was founded in 1996 to fundamentally shift the way pesticides are used in California.  

**Project**: OSL worked with CPR to develop a new data tool to track and visualize pesticide use across the state of California at multiple spatial scales, including neighborhoods, school districts and counties. This project leverages publicly available pesticide use data to deliver a tracking tool that remains sustainable and stable online, and transparent to update and maintain by CPR and its coalition partners. 

## Data Biography
- PUR data `data/pur/` is collected from 2001 to 2021
- Section data `data/sections` is a 2023 vintage of the PUR sections. `sections.parquet` combining all county data
- `data/geo/cb_2021_06_tract_500k.zip` contains 2021 census tracts for california
- `data/geo/zip-county-crosswalk.xlsx` uses Q3 2023 HUD zip to county crosswalk (https://www.huduser.gov/apps/public/uspscrosswalk/home)
- `data/geo/cb_2020_us_zcta520_500k (1).zip` contains 2020 ZCTA boundaries, which are the most recent see https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2021.html#list-tab-1883739534
- `data/geo/CA-townships-2023.geojson` is 2023 CA townships geometries from CA State Geoportal's [Public Land Survey System (PLSS): Township and Range](https://gis.data.ca.gov/datasets/ea19d0ff6d584755b8153701fa8f4346/explore?location=38.905874%2C-120.194561%2C7.15)
- Community & demographic data `data/community vars/` are from [American Community Survey 2021 (5-Year Estimates)](https://www.socialexplorer.com/data/ACS2021_5yr/metadata/), accessed via Social Explorer. Variables include: A04001 Hispanic of Latino by Race, A12002 Highest Educational Attainment for Population 25 Years and Over, A14006 Median Household Income, and A17004 Industry by Occupation for Employed Population 16 Years and Over. 

## Scripts
- `scripts/download_pur.py` helps to download and parse PUR data from 2001 to 2021
- `scripts/download_sections.py` helps to download GIS data
