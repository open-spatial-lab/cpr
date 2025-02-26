# %%
import pandas as pd
from os import path
import geopandas as gpd
from pathlib import Path
CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR.parent / 'data'
# remove col display limit
pd.set_option('display.max_columns', None)
# %%
# columns we want
# ID / GEOID / FIPS / ZCTA / COMTRS / MeridianTownshipRange
# Area
# Population
# Black / AA Population
# Hispanic Population
# Pct Black
# Pct Hispanic
# Median Income
# Percent No High School

def do_standard_transformation(df, id_col="FIPS", also_keep=[]):
  cols_out = [
    "Median HH Income",
    "Pop Total",
    "Pop NH Black",
    "Pct NH Black",
    "Pop Hispanic",
    "Pct Hispanic",
    "Pop NH White",
    "Pct NH White",
    "Pop NH Asian",
    "Pct NH Asian",
    "Pop NH AIAN",
    "Pct NH AIAN",
    "Pop NH NHPI",
    "Pct NH NHPI",
    "Pct No High School",
    "Pct Agriculture"
  ]

  df["Median HH Income"] = round(df["Median Household Income (In 2021 Inflation Adjusted Dollars)"], 0)
  df["Pop Total"] = df["Total Population"]
  df["Pop NH Black"] = df["Total Population: Not Hispanic or Latino: Black or African American Alone"]
  df["Pct NH Black"] = ((df["Total Population: Not Hispanic or Latino: Black or African American Alone"] / df["Total Population"]) * 100).round(3)
  df["Pop Hispanic"] = df["Total Population: Hispanic or Latino"]
  df["Pct Hispanic"] = ((df["Total Population: Hispanic or Latino"] / df["Total Population"]) * 100).round(3)
  df["Pop NH White"] = df["Total Population: Not Hispanic or Latino: White Alone"]
  df["Pct NH White"] = ((df["Total Population: Not Hispanic or Latino: White Alone"] / df["Total Population"]) * 100).round(3)
  df["Pop NH Asian"] = df["Total Population: Not Hispanic or Latino: Asian Alone"]
  df['Pct NH Asian'] = ((df["Total Population: Not Hispanic or Latino: Asian Alone"] / df["Total Population"]) * 100).round(3)
  df["Pop NH AIAN"] = df["Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone"]
  df["Pct NH AIAN"] = ((df["Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone"] / df["Total Population"]) * 100).round(3)
  df["Pop NH NHPI"] = df["Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone"]
  df["Pct NH NHPI"] = ((df["Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone"] / df["Total Population"]) * 100).round(3)
  df["Pct No High School"] = ((df["Population 25 Years and Over: Less than High School"] / df["Population 25 Years and Over:"]) * 100).round(3)
  df["Pct Agriculture"] = ((df["Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining"] / df["Total Employed Civilian Population 16 Years and Over"]) * 100).round(3)

  return df[
    [id_col] + cols_out + also_keep
  ]
# %%
def do_tract():
  tract_demog = do_standard_transformation(
    pd.read_parquet(DATA_DIR / "census_data" / "ca-tract.parquet")
  )
  tract_geo = pd.read_parquet(DATA_DIR / "census_geos" / "ca-tract.parquet")[[
    "GEOID",
    "NAMELSAD",
    "NAMELSADCO",
    "ALAND",
    "AWATER"
  ]]
  merged = tract_geo.merge(tract_demog, left_on="GEOID", right_on="FIPS")
  merged["Area Name"] = merged["NAMELSAD"] + " " + merged["NAMELSADCO"]
  merged = merged.drop(columns=["NAMELSAD", "NAMELSADCO"]).drop_duplicates()
  merged.to_parquet(DATA_DIR / "output" / "ca-tract-demography.parquet")
# %%
standard_rename_dict = {
    "NH Black or African American Alone": "Total Population: Not Hispanic or Latino: Black or African American Alone",
    "Hispanic or Latino": "Total Population: Hispanic or Latino",
    "Population 25 and Over": "Population 25 Years and Over:",
    "Population 25 and Over: Less than High School": "Population 25 Years and Over: Less than High School",
    "Employed: Agriculture, Forestry, Fishing and Hunting, and Mining": "Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining",
    "Employed 16 and Over": "Total Employed Civilian Population 16 Years and Over",
    "NH White Alone": "Total Population: Not Hispanic or Latino: White Alone",
    "NH Asian Alone": "Total Population: Not Hispanic or Latino: Asian Alone",
    "NH American Indian and Alaska Native Alone": "Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone",
    "NH Native Hawaiian and Other Pacific Islander Alone": "Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone",
  }
def do_county():
  county_df = pd.read_parquet(DATA_DIR / "census_data" / "ca-county.parquet")
  county_df = county_df.rename(columns=standard_rename_dict)
  county = do_standard_transformation(
    county_df,
    id_col="FIPS",
    also_keep=["Area Name"]
  )
  county = county.sort_values("Area Name")
  # row number as id
  county['county_cd'] = county.index.astype(int)
  county_geo = pd.read_parquet(DATA_DIR / "census_geos" / "cal_counties.parquet")[[
    "county_cd",
    "NAME",
    "ALAND",
    "AWATER"
  ]]
  county_geo['county_cd'] = county_geo['county_cd'].astype(int)
  merged = county.merge(county_geo, left_on="county_cd", right_on="county_cd")\
    .drop(columns=["county_cd", "NAME"])
  merged["ALAND"] = merged['ALAND'].astype(int)
  merged["AWATER"] = merged['AWATER'].astype(int)
  merged.drop_duplicates().to_parquet(DATA_DIR / "output" / "ca-county-demography.parquet")
# %%
def do_schools():
  schools = pd.read_parquet(DATA_DIR / "census_data" / "ca-school.parquet")
  schools = schools.rename(columns=standard_rename_dict)

  schools = do_standard_transformation(
    schools,
    id_col="FIPS",
    also_keep=["Area Name"]
  )

  schools = schools.sort_values("Area Name")

  schools_geo = pd.read_parquet(DATA_DIR / "census_geos" / "ca-school.parquet")[[
    "FIPS",
    "ALAND",
    "AWATER"
  ]]
  merged = schools.merge(schools_geo, left_on="FIPS", right_on="FIPS")
  merged.drop_duplicates().to_parquet(DATA_DIR / "output" / "ca-school-demography.parquet")
# %%
def do_zips():
  zctas = pd.read_parquet(DATA_DIR / "census_data" / "ca-zip.parquet")
  zctas = do_standard_transformation(
    zctas,
    id_col="GEOID",
    also_keep=[]
  )
  zcta_geo = pd.read_parquet(DATA_DIR / "census_geos" / "ca-zip.parquet")[[
    "ZCTA5CE20",
    "ALAND20",
    'AWATER20'
  ]].rename(columns={
    "ZCTA5CE20": "GEOID",
    "ALAND20": "ALAND",
    "AWATER20": "AWATER"
  })
  merged = zctas.merge(zcta_geo, left_on="GEOID", right_on="GEOID")
  merged.drop_duplicates().to_parquet(DATA_DIR / "output" / "ca-zip-demography.parquet")
# %%
sum_proportion_cols = [
  'Total Population',
  'Total Population: Hispanic or Latino',
  'Total Population: Not Hispanic or Latino: Black or African American Alone',
  'Total Population: Not Hispanic or Latino: White Alone',
  'Total Population: Not Hispanic or Latino: Asian Alone',
  'Total Population: Not Hispanic or Latino: American Indian and Alaska Native Alone',
  'Total Population: Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander Alone',
  "Population 25 Years and Over: Less than High School",
  "Population 25 Years and Over:",
  "Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining",
  "Total Employed Civilian Population 16 Years and Over"
]

average_proportion_cols = [
  'Median Household Income (In 2021 Inflation Adjusted Dollars)',
]

def do_sections():
  sections_geo = gpd.read_parquet(DATA_DIR / "sections" / "sections.parquet")\
    .to_crs("EPSG:3310")
  sections_geo['SECTION_AREA'] = sections_geo['geometry'].area
  sections_geo = pd.DataFrame(sections_geo[['CO_MTRS', 'SECTION_AREA']])
  sections_xwalk = pd.read_parquet(DATA_DIR / "census_geos", "crosswalks" / "sections-to-tracts.parquet")
  sections_geo = sections_geo.rename(columns={"CO_MTRS": "comtrs"})
  tract_demog = pd.read_parquet(DATA_DIR / "census_data" / "ca-tract.parquet")

  merged = sections_geo.merge(sections_xwalk, left_on="comtrs", right_on="CO_MTRS")\
    .merge(tract_demog, left_on="GEOID", right_on="FIPS", how="left")


  combined_cols = [*sum_proportion_cols, *average_proportion_cols]
  prop_cols = [f"{col}_prop" for col in combined_cols]
  for col in [*sum_proportion_cols, *average_proportion_cols]:
    merged[col+"_prop"] = merged[col] * merged['AREA_RATIO']

  merged = merged[[
    "comtrs",
    "AREA_RATIO",
    *prop_cols
  ]]

  merged = merged.fillna(0)
  merged = merged.groupby("comtrs").sum().reset_index()

  for col in prop_cols:
    merged[col] = merged[col].fillna(0).round(0).astype(int)

  merged.columns = [col.replace("_prop", "") for col in merged.columns]

  for col in average_proportion_cols:
    merged[col] = merged[col] / merged['AREA_RATIO']
  merged = do_standard_transformation(merged, id_col="comtrs")
  merged = sections_geo.merge(merged, left_on="comtrs", right_on="comtrs")
  
  merged = merged.rename(columns={"SECTION_AREA": "ALAND"})
  merged.to_parquet(DATA_DIR / "output" / "ca-section-demography.parquet")
# %%
def do_townships():
  townships = gpd.read_file('../data/geo/CA-townships-2023.geojson')\
    .to_crs("EPSG:3310")
  townships['MeridianTownshipRange'] = townships['Meridian'] + " " + townships['TownshipRange']
  townships = townships.dissolve(by='MeridianTownshipRange').reset_index()
  townships = townships[townships['MeridianTownshipRange'].str.len() > 0]
  townships['TOWNSHIP_AREA'] = townships['geometry'].area
  townships = pd.DataFrame(townships[['MeridianTownshipRange', 'TOWNSHIP_AREA']])

  township_xwalk = pd.read_parquet(DATA_DIR / "census_geos" / "crosswalks" / "townships-to-tracts.parquet")
  tract_demog = pd.read_parquet(DATA_DIR / "census_data" / "ca-tract.parquet")

  merged = townships.merge(township_xwalk, left_on="MeridianTownshipRange", right_on="MeridianTownshipRange")\
    .merge(tract_demog, left_on="GEOID", right_on="FIPS", how="left")

  combined_cols = [*sum_proportion_cols, *average_proportion_cols]
  prop_cols = [f"{col}_prop" for col in combined_cols]
  for col in [*sum_proportion_cols, *average_proportion_cols]:
    merged[col+"_prop"] = merged[col] * merged['AREA_RATIO']

  merged = merged[[
    "MeridianTownshipRange",
    "AREA_RATIO",
    *prop_cols
  ]]

  merged = merged.fillna(0)
  merged = merged.groupby("MeridianTownshipRange").sum().reset_index()

  for col in prop_cols:
    merged[col] = merged[col].fillna(0).round(0).astype(int)

  merged.columns = [col.replace("_prop", "") for col in merged.columns]

  for col in average_proportion_cols:
    merged[col] = merged[col] / merged['AREA_RATIO']

  merged = do_standard_transformation(merged, id_col="MeridianTownshipRange")

  merged = townships.merge(merged, left_on="MeridianTownshipRange", right_on="MeridianTownshipRange")
  merged = merged.rename(columns={"TOWNSHIP_AREA": "ALAND"})
  merged.to_parquet(DATA_DIR / "output" / "ca-township-demography.parquet")
# %%
def main():
  do_tract()
  do_county()
  do_schools()
  do_zips()
  do_sections()
  do_townships()

def get_pops(df):
  return {
    "total": df["Pop Total"].sum(),
    "nh_black": df["Pop NH Black"].sum(),
    "hispanic": df["Pop Hispanic"].sum(),
    "nh_white": df["Pop NH White"].sum(),
    "nh_asian": df["Pop NH Asian"].sum(),
    "nh_aian": df["Pop NH AIAN"].sum(),
    "nh_nhpi": df["Pop NH NHPI"].sum()
  }
def get_diffs(pops1, pops2):
  diffs = {}
  for key in pops1.keys():
    diffs[key] = pops1[key] - pops2[key]
    diffs[key + '_pct'] = round(diffs[key] / pops1[key], 4) * 100
  return diffs
# %%
if __name__ == "__main__":
  main()
  tract = pd.read_parquet(DATA_DIR / "output" / "ca-tract-demography.parquet")
  tract_pops = get_pops(tract)

  county = pd.read_parquet(DATA_DIR / "output" / "ca-county-demography.parquet")
  county_pops = get_pops(county)
  county_diff = get_diffs(tract_pops, county_pops)
  print('\n!!!COUNTY\n', county_diff)

  school = pd.read_parquet(DATA_DIR / "output" / "ca-school-demography.parquet")
  school_pops = get_pops(school)
  school_diff = get_diffs(tract_pops, school_pops)
  print('\n!!!SCHOOL\n', school_diff)

  zctas = pd.read_parquet(DATA_DIR / "output" / "ca-zip-demography.parquet").drop_duplicates()
  zcta_pops = get_pops(zctas)
  zcta_diff = get_diffs(tract_pops, zcta_pops)
  print('\n!!!ZCTAS\n', zcta_diff)

  sections = pd.read_parquet(DATA_DIR / "output" / "ca-section-demography.parquet")
  sections_pops = get_pops(sections)
  sections_diff = get_diffs(tract_pops, sections_pops)
  print('\n!!!SECTIONS\n', sections_diff)

  townships = pd.read_parquet(DATA_DIR / "output" / "ca-township-demography.parquet")
  township_pops = get_pops(townships)
  township_diff = get_diffs(tract_pops, township_pops)
  print('\n!!!TOWNSHIPS\n', township_diff)