# %%
import pandas as pd
from os import path
import geopandas as gpd

current_dir = path.dirname(__file__)
data_dir = path.join(current_dir, "..", "data")
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
    "Pax Total",
    "Pax NH Black",
    "Pct NH Black",
    "Pax Hispanic",
    "Pct Hispanic",
    "Pct No High School",
    "Pct Agriculture"
  ]

  df["Median HH Income"] = df["Median Household Income (In 2021 Inflation Adjusted Dollars)"]
  df["Pax Total"] = df["Total Population"]
  df["Pax NH Black"] = df["Total Population: Not Hispanic or Latino: Black or African American Alone"]
  df["Pct NH Black"] = df["Total Population: Not Hispanic or Latino: Black or African American Alone"] / df["Total Population"]
  df["Pax Hispanic"] = df["Total Population: Hispanic or Latino"]
  df["Pct Hispanic"] = df["Total Population: Hispanic or Latino"] / df["Total Population"]
  df["Pct No High School"] = df["Population 25 Years and Over: Less than High School"] / df["Population 25 Years and Over:"]
  df["Pct Agriculture"] = df["Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining"] / df["Total Employed Civilian Population 16 Years and Over"]

  return df[
    [id_col] + cols_out + also_keep
  ]
# %%
tract_demog = do_standard_transformation(
  pd.read_parquet(path.join(data_dir, "census_data", "ca-tract.parquet"))
)
tract_geo = pd.read_parquet(path.join(data_dir, "census_geos", "ca-tract.parquet"))[[
  "GEOID",
  "NAMELSAD",
  "NAMELSADCO",
  "ALAND",
  "AWATER"
]]
merged = tract_geo.merge(tract_demog, left_on="GEOID", right_on="FIPS")
merged["Area Name"] = merged["NAMELSAD"] + " " + merged["NAMELSADCO"]
merged = merged.drop(columns=["NAMELSAD", "NAMELSADCO"])
merged.to_parquet(path.join(data_dir, "output", "ca-tract-demography.parquet"))
# %%
county_df = pd.read_parquet(path.join(data_dir, "census_data", "ca-county.parquet"))
county_df = county_df.rename(columns={
  "NH Black or African American Alone": "Total Population: Not Hispanic or Latino: Black or African American Alone",
  "Hispanic or Latino": "Total Population: Hispanic or Latino",
  "Population 25 and Over": "Population 25 Years and Over:",
  "Population 25 and Over: Less than High School": "Population 25 Years and Over: Less than High School",
  "Employed: Agriculture, Forestry, Fishing and Hunting, and Mining": "Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining",
  "Employed 16 and Over": "Total Employed Civilian Population 16 Years and Over",
  })
county = do_standard_transformation(
  county_df,
  id_col="FIPS",
  also_keep=["Area Name"]
)
county = county.sort_values("Area Name")
# row number as id
county['county_cd'] = county.index.astype(int)
county_geo = pd.read_parquet(path.join(data_dir, "census_geos", "cal_counties.parquet"))[[
  "county_cd",
  "NAME",
  "ALAND",
  "AWATER"
]]
county_geo['county_cd'] = county_geo['county_cd'].astype(int)
merged = county.merge(county_geo, left_on="county_cd", right_on="county_cd")\
  .drop(columns=["county_cd", "NAME"])
merged.to_parquet(path.join(data_dir, "output", "ca-county-demography.parquet"))
# %%
schools = pd.read_parquet(path.join(data_dir, "census_data", "ca-school.parquet"))
schools = schools.rename(columns={
  "NH Black or African American Alone": "Total Population: Not Hispanic or Latino: Black or African American Alone",
  "Hispanic or Latino": "Total Population: Hispanic or Latino",
  "Population 25 and Over": "Population 25 Years and Over:",
  "Population 25 and Over: Less than High School": "Population 25 Years and Over: Less than High School",
  "Employed: Agriculture, Forestry, Fishing and Hunting, and Mining": "Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining",
  "Employed 16 and Over": "Total Employed Civilian Population 16 Years and Over",
  })

schools = do_standard_transformation(
  schools,
  id_col="FIPS",
  also_keep=["Area Name"]
)

schools = schools.sort_values("Area Name")

schools_geo = pd.read_parquet(path.join(data_dir, "census_geos", "ca-school.parquet"))[[
  "FIPS",
  "ALAND",
  "AWATER"
]]
merged = schools.merge(schools_geo, left_on="FIPS", right_on="FIPS")
merged.to_parquet(path.join(data_dir, "output", "ca-school-demography.parquet"))
# %%
zctas = pd.read_parquet(path.join(data_dir, "census_data", "ca-zip.parquet"))
zctas = do_standard_transformation(
  zctas,
  id_col="GEOID",
  also_keep=[]
)
zcta_geo = pd.read_parquet(path.join(data_dir, "census_geos", "ca-zip.parquet"))[[
  "ZCTA5CE20",
  "ALAND20",
  'AWATER20'
]].rename(columns={
  "ZCTA5CE20": "GEOID",
  "ALAND20": "ALAND",
  "AWATER20": "AWATER"
})
merged = zctas.merge(zcta_geo, left_on="GEOID", right_on="GEOID")
merged.to_parquet(path.join(data_dir, "output", "ca-zip-demography.parquet"))
# %%
sections_geo = gpd.read_parquet(path.join(data_dir, "sections", "sections.parquet"))\
  .to_crs("EPSG:3310")
sections_geo['SECTION_AREA'] = sections_geo['geometry'].area
sections_geo = pd.DataFrame(sections_geo[['CO_MTRS', 'SECTION_AREA']])
sections_xwalk = pd.read_parquet(path.join(data_dir, "census_geos", "crosswalks", "sections-to-tracts.parquet"))
sections_geo = sections_geo.rename(columns={"CO_MTRS": "comtrs"})
tract_demog = pd.read_parquet(path.join(data_dir, "census_data", "ca-tract.parquet"))

merged = sections_geo.merge(sections_xwalk, left_on="comtrs", right_on="CO_MTRS")\
  .merge(tract_demog, left_on="GEOID", right_on="FIPS", how="left")

sum_proportion_cols = [
  'Total Population',
  'Total Population: Hispanic or Latino',
  'Total Population: Not Hispanic or Latino: Black or African American Alone',
  "Population 25 Years and Over: Less than High School",
  "Population 25 Years and Over:",
  "Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining",
  "Total Employed Civilian Population 16 Years and Over"
]

average_proportion_cols = [
  'Median Household Income (In 2021 Inflation Adjusted Dollars)',
]

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
merged.to_parquet(path.join(data_dir, "output", "ca-section-demography.parquet"))
# %%
townships = gpd.read_file('../data/geo/CA-townships-2023.geojson')\
  .to_crs("EPSG:3310")
# %%
sections_geo['SECTION_AREA'] = sections_geo['geometry'].area
sections_geo = pd.DataFrame(sections_geo[['CO_MTRS', 'SECTION_AREA']])
sections_xwalk = pd.read_parquet(path.join(data_dir, "census_geos", "crosswalks", "sections-to-tracts.parquet"))
sections_geo = sections_geo.rename(columns={"CO_MTRS": "comtrs"})
tract_demog = pd.read_parquet(path.join(data_dir, "census_data", "ca-tract.parquet"))

merged = sections_geo.merge(sections_xwalk, left_on="comtrs", right_on="CO_MTRS")\
  .merge(tract_demog, left_on="GEOID", right_on="FIPS", how="left")

sum_proportion_cols = [
  'Total Population',
  'Total Population: Hispanic or Latino',
  'Total Population: Not Hispanic or Latino: Black or African American Alone',
  "Population 25 Years and Over: Less than High School",
  "Population 25 Years and Over:",
  "Employed Civilian Population 16 Years and Over: Agriculture, Forestry, Fishing and Hunting, and Mining",
  "Total Employed Civilian Population 16 Years and Over"
]

average_proportion_cols = [
  'Median Household Income (In 2021 Inflation Adjusted Dollars)',
]

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
merged.to_parquet(path.join(data_dir, "output", "ca-section-demography.parquet"))