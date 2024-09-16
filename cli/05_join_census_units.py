# %%
#### IMPORT DEPS
import pandas as pd
from os import path

current_dir = path.dirname(__file__)
data_dir = path.join(current_dir, "..", "data")

should_be_numeric = [
  "lbs_prd_used",
  "lbs_chm_used",
  "amount_planted"
]
# %%
#### IMPORT DATA
calpip_data = pd.read_parquet(path.join(data_dir, "calpip", "calpip_grouped.parquet"))
categories = pd.read_parquet(path.join(data_dir, "meta", "categories.parquet"))
#### MERGE WITH CATEGORIES AND OUTPUT sections
calpip_data = calpip_data.merge(categories, left_on="chem_code", right_on="chem_code", how="left")
calpip_data.to_parquet(path.join(data_dir, "output", "calpip-sections.parquet"), compression='gzip')
# %%
#### CONFIG
crosswalk_cols = [
  'lbs_prd_used',
  'lbs_chm_used',
]

default_demog_calculated_cols = [
      {
        "name": "median_hh",
        "column": lambda x: x["Median Household Income (In 2021 Inflation Adjusted Dollars)"]
      },
      {
        "name": "pct_black",
        "column": lambda x: 0 if x['Total Population'] == 0 else x["Total Population: Not Hispanic or Latino: Black or African American Alone"] / x['Total Population']
      },
      {
        "name": "pct_hispanic",
        "column": lambda x: 0 if x['Total Population'] == 0 else x["Total Population: Hispanic or Latino"] / x['Total Population']
      }
]

xwalk_config = [
  {
    "name": "tract",
    "crosswalk": path.join(data_dir, "census_geos", "crosswalks", "tract_intersections.parquet"),
    "crosswalk_id": "GEOID",
    "crosswalk_ratio_col": "AREA_RATIO",
    "demog": path.join(data_dir, "census_data", "ca-tract.parquet"),
    "demog_id": "FIPS",
    "demog_columns": [*default_demog_calculated_cols],
    "usetype": ['AG'],
    "outdir": path.join(data_dir, "output", "calpip-tract.parquet")
  },
  {
    "name": "school districts",
    "crosswalk": path.join(data_dir, "census_geos", "crosswalks", "school_district_intersections.parquet"),
    "crosswalk_id": "FIPS",
    "crosswalk_ratio_col": "AREA_RATIO",
    "demog": path.join(data_dir, "census_data", "ca-school.parquet"),
    "demog_id": "FIPS",
    "demog_columns": [
      {
        "name": "median_hh",
        "column": lambda x: x["Median Household Income (In 2021 Inflation Adjusted Dollars)"]
      },
      {
        "name": "pct_black",
        "column": lambda x: 0 if x['Total Population'] == 0 else x["NH Black or African American Alone"] / x['Total Population']
      },
      {
        "name": "pct_hispanic",
        "column": lambda x: 0 if x['Total Population'] == 0 else x["Hispanic or Latino"] / x['Total Population']
      }],
    "usetype": ['AG'],
    "outdir": path.join(data_dir, "output", "calpip-school.parquet")
  },
  {
    "name": "zip",
    "crosswalk": path.join(data_dir, "census_geos", "crosswalks", "zip_intersections.parquet"),
    "crosswalk_id": "ZCTA5CE20",
    "crosswalk_ratio_col": "AREA_RATIO",
    "demog": path.join(data_dir, "census_data", "ca-zip.parquet"),
    "demog_id": "GEOID",
    "demog_columns": [*default_demog_calculated_cols],
    "usetype": ['AG'],
    "outdir": path.join(data_dir, "output", "calpip-zip.parquet")
  },
]

# %%
#### DO CENSUS JOINS
for c in xwalk_config:
  crosswalk = pd.read_parquet(c["crosswalk"])
  pur = calpip_data.copy()
  pur = pur[pur['usetype'].isin(c['usetype'])]

  crosswalked = pur.merge(crosswalk, left_on="comtrs", right_on="CO_MTRS", how="left")
  crosswalked = crosswalked.drop(columns=['CO_MTRS'])

  for col in crosswalk_cols:
    crosswalked[col] = crosswalked[c['crosswalk_ratio_col']] * crosswalked[col]
  
  demog = pd.read_parquet(c["demog"])
  for col_config in c["demog_columns"]:
    demog[col_config["name"]] = demog.apply(col_config["column"], axis=1)
  demog = demog[[c["demog_id"], *[x['name'] for x in c['demog_columns']]]]

  joined = crosswalked.merge(demog, left_on=c['crosswalk_id'], right_on=c["demog_id"], how="left")
  
  if c['demog_id'] != c['crosswalk_id']:
    joined = joined.drop(columns=[c["demog_id"]])

  joined.to_parquet(c['outdir'], compression='gzip')
# %%
#### DO COUNTY AGG
default_table_cols = [
      'aerial_ground', 'usetype', 
      'chem_code', 'county_cd', 'prodchem_pct',
      'prodno', 'site_code',  'monthyear',
      'ai_class','ai_type','health','risk', 'major_category'
]

county_config = {
    "name": "county",
    "agg_cols": default_table_cols,
    "drop_cols": ['MeridianTownshipRange', 'comtrs'],
    "demog": path.join(data_dir, "census_data", "ca-county.parquet"),
    "demog_id": "FIPS",
    "demog_columns": [
      {
        "name": "median_hh",
        "column": lambda x: x["Median Household Income (In 2021 Inflation Adjusted Dollars)"]
      },
      {
        "name": "pct_black",
        "column": lambda x: 0 if x['Total Population'] == 0 else x["NH Black or African American Alone"] / x['Total Population']
      },
      {
        "name": "pct_hispanic",
        "column": lambda x: 0 if x['Total Population'] == 0 else x["Hispanic or Latino"] / x['Total Population']
      }],
    "usetype": ['AG', 'NON-AG'],
    "outdir": path.join(data_dir, "output", "calpip-county.parquet")
}

county_agg = calpip_data.copy()\
  .drop(columns=county_config["drop_cols"])\
  .fillna('')\
  .groupby(county_config["agg_cols"]).sum().reset_index()

demog_data = pd.read_parquet(county_config["demog"])

for col_config in county_config["demog_columns"]:
  demog_data[col_config["name"]] = demog_data.apply(col_config["column"], axis=1)

county_fips_xwalk = pd.read_csv(path.join(data_dir, "census_data", "ca-county-dpr-xwalk.csv"),
                                dtype={"FIPS": str})
county_fips_xwalk['DPR_ID'] = county_fips_xwalk['DPR_ID'].astype(str)

county_agg = county_agg.merge(county_fips_xwalk[['DPR_ID','FIPS']], left_on="county_cd", right_on="DPR_ID", how="left")
county_agg = county_agg.drop(columns=["DPR_ID"])
county_agg = county_agg.merge(demog_data[[
  county_config["demog_id"], 
  *[x['name'] for x in county_config['demog_columns']]
]], left_on="FIPS", right_on=county_config["demog_id"], how="left")

for col in should_be_numeric:
  county_agg[col] = pd.to_numeric(county_agg[col], errors='coerce')
county_agg.to_parquet(county_config["outdir"], compression='gzip')
# %%
#### DO CA TO CENSUS AGG
ca_to_census_configs = [
#   {
#     "name": "township",
#     "agg_cols": ["MeridianTownshipRange", *default_table_cols],
#     "agg_id_col": "MeridianTownshipRange",
#     "drop_cols": ['comtrs'],
#     "usetype": ['AG'],
#     "demog_columns": [*default_demog_calculated_cols],
#     "outdir": path.join(data_dir, "output", "calpip-township.parquet"),
#     "xwalk": path.join(data_dir, "census_geos", "crosswalks", "townships-to-tracts.parquet")
# },
  {
    "name": "section",
    "agg_cols": ["MeridianTownshipRange", "comtrs", *default_table_cols],
    "agg_id_col": "comtrs",
    "drop_cols": [],
    "usetype": ['AG', 'NONAG'],
    "demog_columns": [*default_demog_calculated_cols],
    "outdir": path.join(data_dir, "output", "calpip-sections.parquet"),
    "xwalk": path.join(data_dir, "census_geos", "crosswalks", "sections-to-tracts.parquet")
} ,
]

for config in ca_to_census_configs:
  print(config["name"])
  agg = calpip_data.copy()\
    .fillna('')\
    .drop(columns=config["drop_cols"])

  for col in should_be_numeric:
    agg[col] = pd.to_numeric(agg[col], errors='coerce')

  agg = agg.groupby(config["agg_cols"]).sum().reset_index()

  tract_demog = pd.read_parquet(path.join(data_dir, "census_data", "ca-tract.parquet"))
  xwalk = pd.read_parquet(config['xwalk'])

  if "CO_MTRS" in xwalk.columns:
    xwalk = xwalk.rename(columns={"CO_MTRS": "comtrs"})

  merged = xwalk.merge(tract_demog, left_on="GEOID", right_on="FIPS", how="left")

  sum_proportion_cols = [
    'Total Population',
    'Total Population: Hispanic or Latino',
    'Total Population: Not Hispanic or Latino: Black or African American Alone',
  ]

  average_proportion_cols = [
    'Median Household Income (In 2021 Inflation Adjusted Dollars)',
  ]

  for col in [*sum_proportion_cols, *average_proportion_cols]:
    merged[col+"_prop"] = merged[col] * merged['AREA_RATIO']

  prop_cols = [
    *[f"{col}_prop" for col in sum_proportion_cols],
    *[f"{col}_prop" for col in average_proportion_cols],
  ]

  merged = merged[[
    config["agg_id_col"],
    "AREA_RATIO",
    *prop_cols
  ]]
  
  merged = merged.fillna(0)
  merged = merged.groupby(config["agg_id_col"]).sum().reset_index()

  for col in prop_cols:
    merged[col] = merged[col].fillna(0).round(0).astype(int)

  merged.columns = [col.replace("_prop", "") for col in merged.columns]

  for col_config in config["demog_columns"]:
    print(col_config["name"])
    merged[col_config["name"]] = merged.apply(col_config["column"], axis=1)

  merged = merged[[
    config["agg_id_col"],
    *[x['name'] for x in config['demog_columns']]
  ]]

  agg_merged = agg.merge(merged, left_on=config["agg_id_col"], right_on=config["agg_id_col"], how="left")

  for col in should_be_numeric:
    agg_merged[col] = pd.to_numeric(agg_merged[col], errors='coerce')

  agg_merged.to_parquet(config["outdir"], compression='gzip')
# %%
#### SANITY CHECK
total_lbs_chemical = calpip_data['lbs_chm_used'].sum()
total_lbs_ag = calpip_data[calpip_data['usetype'] == 'AG']['lbs_chm_used'].sum()

def print_check(total1, total2):
  diff = total1 - total2
  pct_dif = round(diff / total1, 4) * 100
  print (f"Total lbs: {total1} vs. total lbs: {total2}")
  print(f"Diff: {diff} ({pct_dif}%)")

# import section data
section = pd.read_parquet(path.join(data_dir, "output", "calpip-sections.parquet"))
print("sections\n")
print_check(total_lbs_chemical, section['lbs_chm_used'].sum())
print_check(total_lbs_ag, section[section['usetype'] == 'AG']['lbs_chm_used'].sum())

# import tract data
print("\ntract\n")
tract = pd.read_parquet(path.join(data_dir, "output", "calpip-tract.parquet"))
print_check(total_lbs_ag, tract['lbs_chm_used'].sum())

# import school data
school = pd.read_parquet(path.join(data_dir, "output", "calpip-school.parquet"))
print("\nschool\n")
print_check(total_lbs_ag, school['lbs_chm_used'].sum())

# import zip data
zip = pd.read_parquet(path.join(data_dir, "output", "calpip-zip.parquet"))
print("\nzip\n")
print_check(total_lbs_ag, zip['lbs_chm_used'].sum())

# import township
township = pd.read_parquet(path.join(data_dir, "output", "calpip-township.parquet"))
print("\ntownship\n")
print_check(total_lbs_chemical, township['lbs_chm_used'].sum())
# ag total
township_ag_total = township[township['usetype'] == 'AG']['lbs_chm_used'].sum()
print_check(total_lbs_ag, township_ag_total)

# import county
county = pd.read_parquet(path.join(data_dir, "output", "calpip-county.parquet"))
print("\ncounty\n")
print_check(total_lbs_chemical, county['lbs_chm_used'].sum())
print_check(total_lbs_ag, county[county['usetype'] == 'AG']['lbs_chm_used'].sum())
# %%