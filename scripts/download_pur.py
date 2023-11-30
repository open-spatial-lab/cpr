# %%
import pandas as pd
# zip lib
import zipfile
import wget
from glob import glob
import os
from tqdm import tqdm
import numpy as np
pd.set_option('display.max_columns', None)
# %%
def get_file(year: int) -> pd.DataFrame: 
  # url = f"https://files.cdpr.ca.gov/pub/outgoing/pur_archives/pur{year}.zip"
  # wget.download(url, f"../data/pur/pur{year}.zip")
  # # open zip
  # with zipfile.ZipFile(f"../data/pur/pur{year}.zip", 'r') as zip_ref:
  #   zip_ref.extractall(f"../data/pur/pur{year}")
  pur_data = glob(f"../data/pur/pur{year}/udc*.txt")
  df = pd.concat([pd.read_csv(f) for f in pur_data])
  for col in ['township']:
    df[col] = df[col].astype(str)
  df.to_csv(f"../data/pur/pur{year}.csv", index=False)
  # os.remove(f"../data/pur/pur{year}.zip")
  return df

# %%
for year in range(2001, 2022):
  get_file(year)
# %%
def open_text_at_year(filename:str, year: int) -> pd.DataFrame:
  return pd.read_csv(f"../data/pur/pur{year}/{filename}.txt")

def concat_years(filename: str) -> pd.DataFrame:
  df = pd.DataFrame()
  for year in range(2001, 2022):
    try:
      df = pd.concat([df, open_text_at_year(filename, year)])
    except:
      print(f"Error with {filename} at {year}")
  return df
# %%
def combine_codes_over_years():
  chemical = concat_years("chemical")
  product = concat_years("product")
  formula = concat_years("formula")
  chem_cas = concat_years("chem_cas")
  qualify = concat_years("qualify")
  site = concat_years("site")

  return {
    "chemical": chemical,
    "product": product,
    "formula": formula,
    "chem_cas": chem_cas,
    "qualify": qualify,
    "site": site
  }
# %%
combined_codes = combine_codes_over_years()
# %%
prod_clean = combined_codes['product'].drop_duplicates(keep="first")
# %%
# combine all pur*.csv in data/pur
pur = pd.concat([pd.read_csv(f) for f in tqdm(glob("../data/pur/pur*.csv"))])
# %%
pur['usetype'] = pur['comtrs'].apply(lambda x: 'ag' if type(x) == str else 'nonag')
# %%

# remove pandas col limit
pd.set_option('display.max_columns', None)
pur.head()
# %%
# %%
cols_to_sanitize = ['applic_dt','unit_of_meas','unit_planted','applic_time', 'base_ln_mer', 'township', 'tship_dir', 'range', 'section',
                    'site_loc_id','grower_id','license_no', 'aer_gnd_ind', 'document_no', 'record_id', 'comtrs', 'error_flag'
                    ]
for col in cols_to_sanitize:
  pur[col] = pur[col].astype(str)
# %%

pur = pd.read_parquet("../data/pur/pur_full.parquet")
# pur.to_parquet("../data/pur/pur_full.parquet")
# %%
# convert applic_dt to datetime
from tqdm import tqdm
import re
# %%
# convert applic_dt to datetime
pur['applic_dt'] = pur['applic_dt'].replace('nan', None)
pur['date'] = pd.to_datetime(pur['applic_dt'], format="%m/%d/%Y", errors="coerce")
# %%
pur['usetype'] = pur['comtrs'].apply(lambda x: 'ag' if type(x) == str else 'nonag')
# %%
cols = ["use_no",
"prodno",
"county_cd",
"chem_code",
"prodchem_pct",
"lbs_chm_used",
"lbs_prd_used",
"site_loc_id",
"site_code",
"grower_id",
"license_no",
"aer_gnd_ind",
"comtrs",
"error_flag",
"date",
"usetype"]
pur_min = pur[cols]
# %%
pur_min['county_cd'] = pur_min['county_cd'].apply(lambda x: str(x).zfill(3))
pur_min['county_cd'] = '06' + pur_min['county_cd']
# %%
pur_min = pur_min[pur_min['date'] >= '2010-01-01']
# %%
key_join_codes = pd.read_parquet('../data/geo/sections_key_join.parquet')
key_join_codes = key_join_codes.drop(columns=['COUNTY'])
# %%
pur_min = pur_min.merge(key_join_codes, left_on='comtrs', right_on='CO_MTRS', how="left")
pur_min = pur_min.rename(columns={'county_id':'COUNTY'})
# %%
pur_min.to_parquet("../data/pur/pur_min.parquet", compression="brotli")
# %%
import json
with open("../data/pur/comtrs_dump.json", 'w') as fp:
  # write json x
  json.dump(list(pur_min['comtrs'].unique()), fp)
# %%
pur_min2 = pur_min[pur_min['date'] >= '2017-01-01']
pur_min2 = pur_min[pur_min['date'] >= '2017-01-01']
# %%
pur_min2 = pd.read_parquet("../data/pur/pur_min2.parquet")
# pur_min2.to_parquet("../data/pur/pur_min2.parquet")
# %%
key_join_codes = pd.read_parquet('../data/geo/sections_key_join_school_district.parquet')[["CO_MTRS", "SCID", "TOWNSHIP"]]
# %%
cols_to_drop = [
  # "use_no",
  # "site_loc_id",
  # "license_no",
  # "aer_gnd_ind",
  # "error_flag",
  # "ELSD",
  # "SCSD",
  # "UNSD",
  # "TOWNSHIP"
]
# pur_min2 = pur_min2.drop(columns=cols_to_drop)
# %%
pur_min2['usetype'] = pur_min2['comtrs'].apply(lambda x: 'nonag' if x == 'nan' else 'ag')
# %%
pur_min2 = pur_min2.merge(key_join_codes, left_on='comtrs', right_on='CO_MTRS', how="left")
# %%
# drop CO_MTRS
pur_min2 = pur_min2.drop(columns=['CO_MTRS_x', 'CO_MTRS_y'])
# %%
pur_min2['year'] = pur_min2['date'].apply(lambda x: x.year)
# %%
pur_min2.to_parquet("../data/pur/pur_min2.parquet", compression="zstd")
# %%
zips = list(key_join_codes.ZIP.unique())
# UNSD = key_join_codes.UNSD.unique()

# %%
counties = pd.read_csv("../data/R13494891_SL050.csv")
# %%
counties = counties[['FIPS','Qualifying Name']]
labels = []
for i, row in counties.iterrows():
  labels.append({
    "value": row['FIPS'],
    "label": row['Qualifying Name']
  })
# %%
import json
with open("../data/pur/county_labels.json", 'w') as fp:
  json.dump(labels, fp)

with open("../data/pur/zips.json", 'w') as fp:
  json.dump(zips, fp)
# %%
