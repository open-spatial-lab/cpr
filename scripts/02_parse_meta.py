# %%
import json
import pandas as pd
from os import path
from bs4 import BeautifulSoup
import requests
data_dir = "../data"
# %%
calpip_data = pd.read_parquet(path.join(data_dir, "calpip", "calpip_grouped.parquet"))
# %%
json_path = "../data/pur/pur_codes.json"
# %%
with open(json_path, 'r') as fp:
  codes = json.load(fp)[1]
# %%
config = [
  {
    "codes_key": "chemlookup",
    "drop_cols": ["cas_number"],
    "dedup_cols": ["chem_code"],
    "id_col": "chem_code",
    "calpip_active_col": "chem_code",
    "outpath": "../data/meta/chemicals.parquet"
  },
  # {
  #   "codes_key": "inertlookup",
  #   "drop_cols": [],
  #   "dedup_cols": ["inert_code"],
  #   "id_col": "inert_code",
  #   "calpip_active_col": "inert_code",
  #   "outpath": "../data/meta/inerts.parquet"
  # },
  {
    "codes_key": "prodlookup",
    "drop_cols": [],
    "dedup_cols": ["product_code"],
    "id_col": "product_code",
    "calpip_active_col": "prodno",
    "outpath": "../data/meta/products.parquet"
  },
  {
    "codes_key": "sitelookup",
    "drop_cols": [],
    "dedup_cols": ["site_code"],
    "id_col": "site_code",
    "calpip_active_col": "site_code",
    "outpath": "../data/meta/sites.parquet"
  },
  # {
  #   "codes_key": "errorlookup",
  #   "drop_cols": [],
  #   "dedup_cols": ["error_code"],
  #   "id_col": "error_code",
  #   "calpip_active_col": "error_code",
  #   "outpath": "../data/meta/errors.parquet"
  # },

]

def handle_cleanup(codes, config, calpip_data=calpip_data):
  df = pd.DataFrame(codes[config['codes_key']])
  df = df.drop(columns=config['drop_cols'])
  df = df.drop_duplicates(keep="first", subset=config['dedup_cols'])
  df[config['id_col']] = df[config['id_col']].astype(str)
  current_entries = calpip_data[config['calpip_active_col']].unique()
  df = df[df[config['id_col']].isin(current_entries)]
  df.to_parquet(config['outpath'])

for c in config:
  handle_cleanup(codes, c)

# %%
# bs4
# %%
# %%
def get_label_dict(df, label_col, value_col):
  inner_df = df[[label_col]].drop_duplicates(keep="first").sort_values(by=[label_col])
  # drop duplicates
  items = []
  for i in range(0, len(inner_df)):
    label = inner_df.iloc[i][label_col]
    value = inner_df.iloc[i][label_col]
    if label is not None and value is not None:
      items.append({
        "label": f"{label}",
        "value": f"{label}"
      })
  return items
# %%
site_labels = get_label_dict(sites, 'site_name', 'site_code')
ai_class = get_label_dict(chemicals, 'ai_class', 'chem_code')
ai_type = get_label_dict(chemicals, 'ai_type', 'chem_code')
ai_type_specific = get_label_dict(chemicals, 'ai_type_specific', 'chem_code')
# %%

with open(f"../data/legacy/site_labels.json", 'w') as fp:
  json.dump(site_labels, fp)

with open(f"../data/legacy/ai_class.json", 'w') as fp:
  json.dump(ai_class, fp)

with open(f"../data/legacy/ai_type.json", 'w') as fp:
  json.dump(ai_type, fp)

with open(f"../data/legacy/ai_type_specific.json", 'w') as fp:
  json.dump(ai_type_specific, fp)
# %%
