# %%
import geopandas as gpd
import pandas as pd
# pd remove col limit
pd.set_option('display.max_columns', None)

# %%
crosswalk = pd.read_excel('../data/geo/zip-county-crosswalk.xlsx')
zip = gpd.read_file('../data/geo/cb_2020_us_zcta520_500k (1).zip')
counties = gpd.read_parquet('../data/geo/cal_counties.parquet')
# pad to 3 characters
counties['county_cd'] = counties['county_cd'].apply(lambda x: str(x).zfill(3))
counties['county_cd'] = '06' + counties['county_cd']
counties = counties.rename(columns={'county_cd':'COUNTY'})
counties.crs = "epsg:4326"
# pad county fips with leading zeros to 5 characters
crosswalk['COUNTY'] = crosswalk['COUNTY'].apply(lambda x: str(x).zfill(5))
crosswalk['ZIP'] = crosswalk['ZIP'].apply(lambda x: str(x).zfill(5))
crosswalk = crosswalk[crosswalk.COUNTY.str.startswith('06')]
zip = zip.merge(crosswalk, left_on='ZCTA5CE20', right_on='ZIP').to_crs("epsg:4326")
zip.to_parquet('../data/geo/ca_zip_codes.parquet')
# %%
tracts = gpd.read_file('../data/geo/cb_2021_06_tract_500k.zip').to_crs('epsg:4326')
tracts.to_parquet('../data/geo/ca_tracts.parquet')
# %%
sections = gpd.read_parquet('../data/sections/sections.parquet')
# to California state plane
sections = sections.to_crs('EPSG:2227')
sections['point'] = sections['geometry'].representative_point()
# to 4326
sections['polygon'] = sections['geometry']
sections['geometry'] = sections['point']
sections = sections.to_crs('EPSG:4326')
try:
  # drop GEOID ZIP ELSD SCSD UNSD COUNTy
  sections = sections.drop(columns=['GEOID', 'ZIP', 'ELSD', 'SCSD', 'UNSD', 'COUNTY'])
except:
  pass
# sections = sections.drop_duplicates(subset="CO_MTRS")
sections = sections.drop_duplicates()
# %%
elsd = gpd.read_file('../data/geo/cb_2021_06_elsd_500k.zip').to_crs('epsg:4326')
scsd = gpd.read_file('../data/geo/cb_2021_06_scsd_500k.zip').to_crs('epsg:4326')
unsd = gpd.read_file('../data/geo/cb_2021_06_unsd_500k.zip').to_crs('epsg:4326')
elsd = elsd.rename(columns={'GEOID':'ELSD'})
scsd = scsd.rename(columns={'GEOID':'SCSD'})
unsd = unsd.rename(columns={'GEOID':'UNSD'})

# %%
# counties = gpd.read_parquet('../data/geo/ca-county.parquet')
# %%
sjoins = [
  tracts[['geometry', 'GEOID']],
  zip[['geometry', 'ZIP']],
  elsd[['geometry', 'ELSD']],
  scsd[['geometry', 'SCSD']],
  unsd[['geometry', 'UNSD']],
  # counties[['geometry', 'COUNTY']]
]

for join in sjoins:
  if "index_left" in sections.columns:
    sections = sections.drop(columns=['index_left'])  
  if "index_right" in sections.columns:
    sections = sections.drop(columns=['index_right'])
  sections = gpd.sjoin(sections, join, how='left')
  if "index_left" in sections.columns:
    sections = sections.drop(columns=['index_left'])  
  if "index_right" in sections.columns:
    sections = sections.drop(columns=['index_right'])

sections = sections.drop_duplicates()
# # %%
# elsd_min = elsd[['ELSD', 'NAME', 'geometry']].rename(columns={'ELSD':'id', 'NAME':'name'})
# elsd_min['Area Type'] = "Elementary School District"
# scsd_min = scsd[['SCSD', 'NAME', 'geometry']].rename(columns={'SCSD':'id', 'NAME':'name'})
# scsd_min['Area Type'] = "Secondary School District"
# unsd_min = unsd[['UNSD', 'NAME', 'geometry']].rename(columns={'UNSD':'id', 'NAME':'name'})
# unsd_min['Area Type'] = "Unified School District"
# tracts['combined_name'] = tracts['NAMELSAD'] +", " + tracts['NAMELSADCO']
# tracts_min = tracts[['GEOID', 'combined_name', 'geometry']].rename(columns={'GEOID':'id', 'combined_name':'name'}).to_crs("epsg:4326")
# tracts_min['Area Type'] = "Census Tract"
# zip['combined_name'] = zip['ZIP'] + ", " + zip['USPS_ZIP_PREF_CITY']
# zip_min = zip[['ZIP', 'combined_name', 'geometry']].rename(columns={'ZIP':'id', 'combined_name':'name'}).to_crs("epsg:4326")
# zip_min['Area Type'] = "Zip Code"
# counties_min = counties[['COUNTY', 'NAME', 'geometry']].rename(columns={'COUNTY':'id', 'NAME':'name'})
# counties_min['Area Type'] = "County"
# # %%
# all_geoms = pd.concat([
#   elsd_min,
#   scsd_min,
#   unsd_min,
#   tracts_min,
#   zip_min,
#   counties_min
# ])
# # %%
# all_geoms.to_parquet('../data/geo/ca_geoms.parquet')
# %%
sections["SCID"] = sections["ELSD"].fillna(sections["UNSD"])
# clip first two characters
sections["SCID"] = sections["SCID"].apply(lambda x: str(x)[2:])
# drop ELSD SCSD UNSD
key_join_info = sections[['CO_MTRS', 'GEOID', "ZIP", "SCID","TOWNSHIP"]]
key_join_info.to_parquet('../data/geo/sections_key_join.parquet')
# key_join_info = pd.read_parquet('../data/geo/sections_key_join.parquet')
# %%
# use UNSD or ELSD
key_join_info['SCHOOL_DISTRICT'] = key_join_info['UNSD'].fillna(key_join_info['ELSD'])
# slice first two characters off
key_join_info['SCHOOL_DISTRICT'] = key_join_info['SCHOOL_DISTRICT'].apply(lambda x: str(x)[2:])
# drop ELSD, SCSD, UNSD
key_join_info = key_join_info.drop(columns=['ELSD', 'SCSD', 'UNSD', "TOWNSHIP"])
# %%
sections_joined = sections.merge(key_join_info, left_on='CO_MTRS', right_on='CO_MTRS')

# %%
sections['geometry'] = sections['polygon']
sections = sections.drop(columns=['polygon'])
# %%
sections.to_parquet('../data/sections/sections.parquet')
# %%

CA_ZIPS = list(zip['ZIP'].unique())
# %%
# county_vars = pd.read_csv("../data/community vars/ca-county.csv")
zip_vars = pd.read_csv("../data/community vars/ca-zip.csv").iloc[1:]
tract_vars = pd.read_csv("../data/community vars/ca-tract.csv")
# %%
zip_vars['GEOID'] = zip_vars['Qualifying Name'].apply(lambda x: x.split(" ")[1])
zip_vars = zip_vars[zip_vars['GEOID'].isin(CA_ZIPS)]
# %%
# as string, pad to 11
tract_vars['FIPS'] = tract_vars['FIPS'].astype(str).apply(lambda x: x.zfill(11))
# %%
zip_vars.to_parquet("../data/community vars/ca-zip.parquet")
tract_vars.to_parquet("../data/community vars/ca-tract.parquet")

# %%
chems = pd.read_csv("../data/pur/pur2021/chemical.txt")
# %%

sections = gpd.read_parquet('../data/sections/sections.parquet')
# use ELSD or UNSD

# %%
key_join_info = sections[['CO_MTRS', 'GEOID', "ZIP", "SCID", "COUNTY","TOWNSHIP"]]
key_join_info.to_parquet('../data/geo/sections_key_join_school_district.parquet')
# %%
