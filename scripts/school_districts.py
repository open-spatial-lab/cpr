# %%
import pandas as pd
import geopandas as gpd
# remove col limit
pd.set_option('display.max_columns', None)
# %%
tracts = pd.read_parquet("../data/community vars/ca-tract.parquet")
# %%
cols_to_keep = ['FIPS',
 'Total Employed Civilian Population 16 Years and Over',
 'Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining',
 'Employed Civilian Population 16 Years and Over: Construction',
 'Employed Civilian Population 16 Years and Over: Manufacturing',
 'Employed Civilian Population 16 Years and Over: Wholesale Trade',
 'Employed Civilian Population 16 Years and Over: Retail Trade',
 'Employed Civilian Population 16 Years and Over: Transportation and Warehousing, and Utilities',
 'Employed Civilian Population 16 Years and Over: Information',
 'Employed Civilian Population 16 Years and Over: Finance and Insurance, and Real Estate and Rental  and Leasing',
 'Employed Civilian Population 16 Years and Over: Professional, Scientific, and Management, and  Administrative and Waste Management Services',
 'Employed Civilian Population 16 Years and Over: Educational Services, and Health Care and Social  Assistance',
 'Employed Civilian Population 16 Years and Over: Arts, Entertainment, and Recreation, and  Accommodation and Food Services',
 'Employed Civilian Population 16 Years and Over: Other Services, Except Public Administration',
 'Employed Civilian Population 16 Years and Over: Public Administration',
 'Median Household Income (In 2021 Inflation Adjusted Dollars)',
 'Total Population',
 'Total Population: Not Hispanic or Latino',
 'Total Population: Not Hispanic or Latino: White Alone',
 'Total Population: Not Hispanic or Latino: Black or African American Alone',
 'Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone',
 'Total Population: Not Hispanic or Latino: Asian Alone',
 'Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone',
 'Total Population: Not Hispanic or Latino: Some Other Race Alone',
 'Total Population: Not Hispanic or Latino: Two or More Races',
 'Total Population: Hispanic or Latino',
 'Total Population: Hispanic or Latino: White Alone',
 'Total Population: Hispanic or Latino: Black or African American Alone',
 'Total Population: Hispanic or Latino: American Indian and Alaska Native Alone',
 'Total Population: Hispanic or Latino: Asian Alone',
 'Total Population: Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone',
 'Total Population: Hispanic or Latino: Some Other Race Alone',
 'Total Population: Hispanic or Latino: Two or More Races',
 'Population 25 Years and Over:',
 'Population 25 Years and Over: Less than High School',
 'Population 25 Years and Over: High School Graduate or More (Includes Equivalency)',
 'Population 25 Years and Over: Some College or More',
 "Population 25 Years and Over: Bachelor's Degree or More",
 "Population 25 Years and Over: Master's Degree or More",
 'Population 25 Years and Over: Professional School Degree or More',
 'Population 25 Years and Over: Doctorate Degree']


# %%
elementary = pd.read_csv("../data/community vars/ca-school-elementary.csv").iloc[1:]
secondary = pd.read_csv("../data/community vars/ca-school-secondary.csv").iloc[1:]
unified = pd.read_csv("../data/community vars/ca-school-unified.csv").iloc[1:]
# %%
for data in [elementary, secondary, unified]:
  for col in data.columns:
    if col == 'FIPS':
      continue
    try:
      data[col] = pd.to_numeric(data[col])
    except:
      pass

# %%
cols_to_keep = [
  'FIPS',
 'Area Name',
 'Total Employed Civilian Population 16 Years and Over',
 'Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining',
 'Employed Civilian Population 16 Years and Over: Construction',
 'Employed Civilian Population 16 Years and Over: Manufacturing',
 'Employed Civilian Population 16 Years and Over: Wholesale Trade',
 'Employed Civilian Population 16 Years and Over: Retail Trade',
 'Employed Civilian Population 16 Years and Over: Transportation and Warehousing, and Utilities',
 'Employed Civilian Population 16 Years and Over: Information',
 'Employed Civilian Population 16 Years and Over: Finance and Insurance, and Real Estate and Rental  and Leasing',
 'Employed Civilian Population 16 Years and Over: Professional, Scientific, and Management, and  Administrative and Waste Management Services',
 'Employed Civilian Population 16 Years and Over: Educational Services, and Health Care and Social  Assistance',
 'Employed Civilian Population 16 Years and Over: Arts, Entertainment, and Recreation, and  Accommodation and Food Services',
 'Employed Civilian Population 16 Years and Over: Other Services, Except Public Administration',
 'Employed Civilian Population 16 Years and Over: Public Administration',
 'Median Household Income (In 2021 Inflation Adjusted Dollars)',
 'Total Population',
 'Total Population: Not Hispanic or Latino: White Alone',
 'Total Population: Not Hispanic or Latino: Black or African American Alone',
 'Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone',
 'Total Population: Not Hispanic or Latino: Asian Alone',
 'Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone',
 'Total Population: Not Hispanic or Latino: Some Other Race Alone',
 'Total Population: Not Hispanic or Latino: Two or More Races',
 'Total Population: Hispanic or Latino',
 'Population 25 Years and Over:',
 'Population 25 Years and Over: Less than High School'
 ]

rename_dict = {
 'Total Employed Civilian Population 16 Years and Over':'Employed 16 and Over',
 'Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining':'Employed: Agriculture, Forestry, Fishing and Hunting, and Mining',
 'Employed Civilian Population 16 Years and Over: Construction':'Employed: Construction',
 'Employed Civilian Population 16 Years and Over: Manufacturing':'Employed: Manufacturing',
 'Employed Civilian Population 16 Years and Over: Wholesale Trade':'Employed: Wholesale Trade',
 'Employed Civilian Population 16 Years and Over: Retail Trade':'Employed: Retail Trade',
 'Employed Civilian Population 16 Years and Over: Transportation and Warehousing, and Utilities':'Employed: Transportation and Warehousing, and Utilities',
 'Employed Civilian Population 16 Years and Over: Information':'Employed: Information',
 'Employed Civilian Population 16 Years and Over: Finance and Insurance, and Real Estate and Rental  and Leasing':'Employed: Finance and Insurance, and Real Estate and Rental  and Leasing',
 'Employed Civilian Population 16 Years and Over: Professional, Scientific, and Management, and  Administrative and Waste Management Services':'Employed: Professional, Scientific, and Management, and  Administrative and Waste Management Services',
 'Employed Civilian Population 16 Years and Over: Educational Services, and Health Care and Social  Assistance':'Employed: Educational Services, and Health Care and Social  Assistance',
 'Employed Civilian Population 16 Years and Over: Arts, Entertainment, and Recreation, and  Accommodation and Food Services':'Employed: Arts, Entertainment, and Recreation, and  Accommodation and Food Services',
 'Employed Civilian Population 16 Years and Over: Other Services, Except Public Administration':'Employed: Other Services, Except Public Administration',
 'Employed Civilian Population 16 Years and Over: Public Administration':'Employed: Public Administration',
 'Median Household Income (In 2021 Inflation Adjusted Dollars)':'Median Household Income (In 2021 Inflation Adjusted Dollars)',
 'Total Population':'Total Population',
 'Total Population: Not Hispanic or Latino: White Alone':'NH White Alone',
 'Total Population: Not Hispanic or Latino: Black or African American Alone':'NH Black or African American Alone',
 'Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone':'NH American Indian and Alaska Native Alone',
 'Total Population: Not Hispanic or Latino: Asian Alone':'NH Asian Alone',
 'Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone':'NH Native Hawaiian and Other Pacific Islander Alone',
 'Total Population: Not Hispanic or Latino: Some Other Race Alone':'NH Some Other Race Alone',
 'Total Population: Not Hispanic or Latino: Two or More Races':'NH Two or More Races',
 'Total Population: Hispanic or Latino':'Hispanic or Latino',
 'Population 25 Years and Over:':'Population 25 and Over',
 'Population 25 Years and Over: Less than High School':'Population 25 and Over: Less than High School'
}

secondary = secondary[cols_to_keep].rename(columns=rename_dict)
elementary = elementary[cols_to_keep].rename(columns=rename_dict)
unified = unified[cols_to_keep].rename(columns=rename_dict)
# %%
# filter fips 699999.0
elementary = elementary[elementary['FIPS'] != '0699999']
secondary = secondary[secondary['FIPS'] != '0699999']
unified = unified[unified['FIPS'] != '0699999']
elementary['District Type'] = 'Elementary'
secondary['District Type'] = 'Secondary'
unified['District Type'] = 'Unified'
# %%
combined = pd.concat([elementary, unified])
# strip off first two characters
combined['FIPS']  = combined['FIPS'].str[2:]
# %%

# %%
combined.to_parquet("../data/community vars/ca-schools-community-vars.parquet")
# %%
elsd_geo = gpd.read_file("../data/geo/cb_2021_06_elsd_500k/cb_2021_06_elsd_500k.shp")
unsd_geo = gpd.read_file("../data/geo/cb_2021_06_unsd_500k/cb_2021_06_unsd_500k.shp")

# %%
# to 4326
elsd_geo = elsd_geo.to_crs("EPSG:4326")
unsd_geo = unsd_geo.to_crs("EPSG:4326")
# rename ELSDLEA to FIPS
elsd_geo = elsd_geo.rename(columns={'ELSDLEA':'FIPS'})
# rename UNSDLEA to FIPS
unsd_geo = unsd_geo.rename(columns={'UNSDLEA':'FIPS'})
# concat
sd_geo = pd.concat([elsd_geo, unsd_geo])
# %%
cols_to_keep = [
  "FIPS", "NAME", "ALAND", "AWATER", "geometry"
]
sd_geo = sd_geo[cols_to_keep]

# %%
sd_geo.to_parquet("../data/community vars/ca-schools-geo.parquet")
# %%
combined = sd_geo.merge(combined, on="FIPS")
# %%
