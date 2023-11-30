# %%
import json
import pandas as pd
# %%
json_path = "../data/pur/pur_codes.json"
# %%
with open(json_path, 'r') as fp:
  codes = json.load(fp)
# %%
errors = pd.DataFrame(codes[1]['errorlookup'])
products = pd.DataFrame(codes[1]['prodlookup'])
chemicals = pd.DataFrame(codes[1]['chemlookup'])
inerts = pd.DataFrame(codes[1]['inertlookup'])
sites = pd.DataFrame(codes[1]['sitelookup'])
# %%
chemicals = chemicals.drop(columns=['cas_number'])
chemicals = chemicals.drop_duplicates(keep="first")
# %%

chems2021 = pd.read_csv("../data/pur/pur2021/chemical.txt")
# %%
legacy_chems = list(chemicals['chem_code'].unique())
# %%
errors.to_parquet("../data/legacy/errors.parquet")
products.to_parquet("../data/legacy/products.parquet")
chemicals.to_parquet("../data/legacy/chemicals.parquet")
inerts.to_parquet("../data/legacy/inerts.parquet")
sites.to_parquet("../data/legacy/sites.parquet")

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
