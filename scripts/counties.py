# %%
import pandas as pd
import geopandas as gpd
# remove col limit
pd.set_option('display.max_columns', None)
# %%
counties = pd.read_csv("../data/community vars/ca-county.csv").iloc[1:]
# %%
cols_to_keep = ['FIPS',
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

for col in counties.columns:
  if col == 'FIPS':
    continue
  try:
    counties[col] = pd.to_numeric(counties[col])
  except:
    pass

# %%

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
counties = counties[cols_to_keep]
counties = counties.rename(columns=rename_dict)
# %%
counties.to_parquet("../data/community vars/ca-county.parquet")
# %%
import geopandas as gpd
# %%
county = gpd.read_file("../data/geo/COUNTY_2019_US_SL050_2019-11-13_15-15-56-579 (2)/COUNTY_2019_US_SL050_Coast_Clipped.shp")
# %%
county = county[county["STATEFP"] == "06"]
# %%
cols_to_keep = [
  "GEOID",
  "NAME",
  'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON',
  "geometry"
]
# %%
county = county[cols_to_keep]
# %%
county = county.rename(columns={"GEOID":"FIPS"})
# %%
county.to_parquet("../data/geo/ca-county.parquet")
# %%
