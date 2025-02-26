# %%
#### IMPORT DEPS
import pandas as pd
from os import path
from pathlib import Path

# pd remove column limit
pd.set_option('display.max_columns', None)
CURRENT_DIR = Path(__file__).parent
DATA_DIR = CURRENT_DIR.parent / "data"

SHOULD_BE_NUMERIC = [
  "lbs_prd_used",
  "lbs_chm_used",
  "amount_planted"
]

CROSSWALK_COLS = [
  'lbs_prd_used',
  'lbs_chm_used',
]

DEFAULT_DEMOG_CALCULATED_COLS = []

#### IMPORT DATA
def get_calpip_full():
  calpip_data = pd.read_parquet(DATA_DIR / "calpip" / "calpip_full.parquet")
  # Generate unique use_id
  calpip_data['use_id'] = calpip_data['monthyear'] + calpip_data['use_number'] + calpip_data['lbs_prd_used'].astype(str) + calpip_data['comtrs']
  calpip_data = calpip_data.drop(columns=['use_number'])
  categories = pd.read_parquet(DATA_DIR / "meta" / "categories.parquet").drop_duplicates()
  ### MERGE WITH CATEGORIES AND OUTPUT sections
  calpip_data = calpip_data.merge(categories, left_on="chem_code", right_on="chem_code", how="left")\
    .sort_values(by=['monthyear', 'use_id'])
  return calpip_data

def minify_use_ids(df, use_id_col="use_id"):
  use_instance_ids = []
  for idx, id in enumerate(df[use_id_col].unique()):
    use_instance_ids.append({
      use_id_col: id,
      'use_index': idx
    })
  use_instance_ids = pd.DataFrame(use_instance_ids)
  return df.merge(use_instance_ids, left_on=use_id_col, right_on=use_id_col, how='left')\
    .drop(columns=[use_id_col])

#### CONFIG
CENSUS_XWALKS = [
  {
    "name": "tract",
    "crosswalk": DATA_DIR / "census_geos" / "crosswalks" / "tract_intersections.parquet",
    "crosswalk_id": "GEOID",
    "crosswalk_ratio_col": "AREA_RATIO",
    "demog": DATA_DIR / "census_data" / "ca-tract.parquet",
    "demog_id": "FIPS",
    "demog_columns": [],
    "usetype": ['AG'],
    "drop_cols": ['comtrs', 'AREA_RATIO', 'MeridianTownshipRange', 'prodchem_pct'],
    "outdir": DATA_DIR / "output" / "calpip-tract.parquet"
  },
  {
    "name": "school districts",
    "crosswalk": DATA_DIR / "census_geos" / "crosswalks" / "school_district_intersections.parquet",
    "crosswalk_id": "FIPS",
    "crosswalk_ratio_col": "AREA_RATIO",
    "demog": DATA_DIR / "census_data" / "ca-school.parquet",
    "demog_id": "FIPS",
    "demog_columns": [],
    "usetype": ['AG'],
    "drop_cols": ['comtrs', 'AREA_RATIO', 'MeridianTownshipRange', 'prodchem_pct'],
    "outdir": DATA_DIR / "output" / "calpip-school.parquet"
  },
  {
    "name": "zip",
    "crosswalk": DATA_DIR / "census_geos" / "crosswalks" / "zip_intersections.parquet",
    "crosswalk_id": "ZCTA5CE20",
    "crosswalk_ratio_col": "AREA_RATIO",
    "demog": DATA_DIR / "census_data" / "ca-zip.parquet",
    "demog_id": "GEOID",
    "demog_columns": [],
    "usetype": ['AG'],
    "drop_cols": ['comtrs', 'AREA_RATIO', 'MeridianTownshipRange', 'prodchem_pct'],
    "outdir": DATA_DIR / "output" / "calpip-zip.parquet"
  },
]

def do_census_joins(calpip_data, xwalk_config=CENSUS_XWALKS, crosswalk_cols=CROSSWALK_COLS):
  #### DO CENSUS JOINS
  for c in xwalk_config:
    print("STARTING ", c["name"])
    crosswalk = pd.read_parquet(c["crosswalk"])
    pur = calpip_data.copy()
    pur = pur[pur['usetype'].isin(c['usetype'])]

    print("DATA READ IN")
    crosswalked = pur.merge(crosswalk, left_on="comtrs", right_on="CO_MTRS", how="left")
    crosswalked = crosswalked.drop(columns=['CO_MTRS'])

    print("CROSSWALK JOINED")
    for col in crosswalk_cols:
      crosswalked[col] = crosswalked[c['crosswalk_ratio_col']] * crosswalked[col]
    print("CROSSWALK FINISHED")
    joined = crosswalked.drop(columns=c['drop_cols'])
    joined['use_id'] = joined['use_id'] + joined[c['crosswalk_id']]
    joined = minify_use_ids(joined)
    joined.to_parquet(c['outdir'], compression='gzip')
    print("EXPORTED ", c['name'])

#### DO COUNTY AGG
COUNTY_CONFIG = {
    "name": "county",
    "drop_cols": ['MeridianTownshipRange', 'comtrs'],
    "demog": DATA_DIR/ "census_data" / "ca-county.parquet",
    "demog_id": "FIPS",
    "demog_columns": [],
    "usetype": ['AG', 'NON-AG'],
    "outdir": DATA_DIR/ "output" / "calpip-county.parquet"
}

def do_county_joins(calpip_data, COUNTY_CONFIG=COUNTY_CONFIG):
  county_agg = calpip_data.copy()\
    .drop(columns=COUNTY_CONFIG["drop_cols"])\
    .fillna('')
  county_fips_xwalk = pd.read_csv(DATA_DIR/ "census_data" / "ca-county-dpr-xwalk.csv",
                                  dtype={"FIPS": str})
  county_fips_xwalk['DPR_ID'] = county_fips_xwalk['DPR_ID'].astype(str)

  county_agg = county_agg.merge(county_fips_xwalk[['DPR_ID','FIPS']], left_on="county_cd", right_on="DPR_ID", how="left")
  county_agg = county_agg.drop(columns=["DPR_ID"])

  for col in SHOULD_BE_NUMERIC:
    county_agg[col] = pd.to_numeric(county_agg[col], errors='coerce')

  county_agg = minify_use_ids(county_agg)
  county_agg.to_parquet(COUNTY_CONFIG["outdir"], compression='gzip')

#### DO CA TO CENSUS AGG
ca_to_census_configs = [
  {
    "name": "township",
    "agg_id_col": "MeridianTownshipRange",
    "drop_cols": ['comtrs'],
    "usetype": ['AG'],
    "demog_columns": [],
    "outdir": DATA_DIR / "output" / "calpip-township.parquet",
    "demog_outdir": DATA_DIR / "output" / "census-township.parquet",
    "xwalk": DATA_DIR / "census_geos" / "crosswalks" / "townships-to-tracts.parquet",
},
  {
    "name": "section",
    "agg_id_col": "comtrs",
    "drop_cols": [],
    "usetype": ['AG', 'NONAG'],
    "demog_columns": [],
    "outdir": DATA_DIR / "output" / "calpip-sections.parquet",
    "demog_outdir": DATA_DIR / "output" / "census-sections.parquet",
    "xwalk": DATA_DIR / "census_geos" / "crosswalks" / "sections-to-tracts.parquet",
}
]

def do_reverse_joins(calpip_data):
  for config in ca_to_census_configs:
    print(config["name"])
    agg = calpip_data.copy()\
      .fillna('')\
      .drop(columns=config["drop_cols"])

    for col in SHOULD_BE_NUMERIC:
      agg[col] = pd.to_numeric(agg[col], errors='coerce')
    agg = minify_use_ids(agg)
    agg.to_parquet(config["outdir"], compression='gzip')

def print_check(total1, total2, prefix=""):
  diff = total1 - total2
  pct_dif = round(diff / total1, 4) * 100
  print (f"{prefix} :: Total lbs :: {total1} vs. total lbs: {total2}")
  print(f"{prefix} :: Diff :: {diff} ({pct_dif}%)")

def do_sanity_checks(calpip_data):
  total_lbs_chemical = calpip_data['lbs_chm_used'].sum()
  total_lbs_ag = calpip_data[calpip_data['usetype'] == 'AG']['lbs_chm_used'].sum()
  total_lbs_prd = calpip_data[['use_id', 'lbs_prd_used']].drop_duplicates()['lbs_prd_used'].sum()
  total_lbs_prd_ag = calpip_data[calpip_data['usetype'] == 'AG'][['use_id', 'lbs_prd_used']].drop_duplicates()['lbs_prd_used'].sum()
  def sanity_check(
      df,
      title,
      ag_only=False
  ): 
    print("Checking ", title)
    if not ag_only:
      print_check(total_lbs_chemical, df['lbs_chm_used'].sum(), f'{title} :: chm')
      print_check(total_lbs_prd, df[['use_index', 'lbs_prd_used']].drop_duplicates()['lbs_prd_used'].sum(), f'{title} :: prd')
    print_check(total_lbs_ag, df[df['usetype'] == 'AG']['lbs_chm_used'].sum(), f'{title} :: chm ag')
    print_check(total_lbs_prd_ag, df[df['usetype'] == 'AG'][['use_index', 'lbs_prd_used']].drop_duplicates()['lbs_prd_used'].sum(), f'{title} :: prd ag')
    return df
  section = sanity_check(
    pd.read_parquet(DATA_DIR / "output" / "calpip-sections.parquet"),
    "Sections"
  )
  school = sanity_check(
    pd.read_parquet(DATA_DIR / "output" / "calpip-school.parquet"),
    "Schools",
    True
  )
  zcta = sanity_check(
    pd.read_parquet(DATA_DIR / "output" / "calpip-zip.parquet"),
    "Zip",
    True
  )
  township = sanity_check(
    pd.read_parquet(DATA_DIR / "output" / "calpip-township.parquet"),
    "Township",
    True
  )
  county = sanity_check(
    pd.read_parquet(DATA_DIR / "output" / "calpip-county.parquet"),
    "County"
  )

  tract = sanity_check(
    pd.read_parquet(DATA_DIR / "output" / "calpip-tract.parquet"),
    "Tract",
    True
  )

def main():
  if not path.exists(DATA_DIR / "output"):
    Path(DATA_DIR / "output").mkdir(parents=True, exist_ok=True)

  calpip_data = get_calpip_full()
  sections = minify_use_ids(calpip_data)
  sections.to_parquet(DATA_DIR / "output" / "calpip-sections.parquet", compression='gzip')
  print("Minified calpip data - doing joins.")
  do_census_joins(calpip_data)
  print("Done with census joins.")
  do_county_joins(calpip_data)
  print("Done with county joins.")
  do_reverse_joins(calpip_data)
  print("Done with reverse joins.")
  do_sanity_checks(calpip_data)
  print("Done with sanity checks.")
# %%
if __name__ == "__main__":
  main()
# %%
