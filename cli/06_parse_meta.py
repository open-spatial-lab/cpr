# %%
import pandas as pd
from os import path
import os

ONLY_USE_ACTIVE = os.getenv("ONLY_USE_ACTIVE", "true") == "true"
# remove max cols
pd.set_option('display.max_columns', None)
data_dir = "../data"
pur_meta_dir = path.join(data_dir, "pur", "2022_meta")


health_labels = [
  {
    "label": "Carcinogen",
    "id": "CARC",
    "columns": ["CARCINOGEN", "CARCINOGEN_ALL"]
  },
  {
    "label": "Groundwater Contaminant",
    "id": "GND",
    "columns": ["GND_WATER", "GND_WATER_ALL"]
  },
  {
    "label": "Reproductive",
    "id": "REPR",
    "columns": ["REPRODUCTIVE", "REPRODUCTIVE_ALL"]
  },
  {
    "label": "Toxic Air Contaminant",
    "id": "TAC",
    "columns": ["TAC", "TAC_ALL"]
  }
]

category_labels = [
  {
    "label": "Organophosphate",
    "id": "OP",
    "columns": ["OP", "OP_ALL"]
  },
  {
    "label": "Carbamate",
    "id": "CARB",
    "columns": ["CARBAMATE", "CARB_ALL"]
  },
  {
    "label": "Pyrethroid",
    "id": "PYR",
    "columns": ["PYRETHROID", "PYR_ALL"]
  },
  {
    "label": "Organochlorine",
    "id": "OCL",
    "columns": ["ORGANOCHLORINE", "ORGANOCHLORINE_ALL"]
  },
  {
    # neonicotinoid
    "label": "Neonicotinoid",
    "id": "NEO",
    "columns": ["NEONICOTINOID"]
  },
  {
    # amide
    "label": "Amide",
    "id": "AM",
    "columns": ["AMIDE"]
  },
  {
    # azole
    "label": "Azole",
    "id": "AZ",
    "columns": ["AZOLE"]
  },
  {
    # botanical
    "label": "Botanical",
    "id": "BOT",
    "columns": ["BOTANICAL"]
  },
  {
    "label": "Chlorinated Phenol",
    "id": "CP",
    "columns": ["CHLORINATED_PHENOL"]
  },
  {
    # fatty_acid
    "label": "Fatty Acid",
    "id": "FA",
    "columns": ["FATTY_ACID"]
  },
  {
    # glycol
    "label": "Glycol",
    "id": "GLY",
    "columns": ["GLYCOL"]
  },
  {
    # halogenated organic
    "label": "Halogenated Organic",
    "id": "HO",
    "columns": ["HALOGENATED_ORGANIC"]
  },
  {
    # inorganic
    "label": "Inorganic",
    "id": "INO",
    "columns": ["INORGANIC"]
  },
  {
    # microbial
    "label": "Microbial",
    "id": "MIC",
    "columns": ["MICROBIAL"]
  },
  {
    # phenol
    "label": "Phenol",
    "id": "PH",
    "columns": ["PHENOL"]
  },
  {
    # phenoxy
    "label": "Phenoxy",
    "id": "PX",
    "columns": ["PHENOXY"]
  },
  {
    # PHEROMONE
    "label": "Pheromone",
    "id": "PR",
    "columns": ["PHEROMONE"]
  },
  {
    # pyrazole
    "label": "Soap",
    "id": "SOAP",
    "columns": ["SOAP"]
  },
  {
    # strobilurin
    "label": "Strobilurin",
    "id": "ST",
    "columns": ["STROBILURIN"]
  },
  {
    # sulfonylurea
    "label": "Sulfonylurea",
    "id": "SU",
    "columns": ["SULFONYLUREA"]
  },
  {
    # triazine
    "label": "Triazine",
    "id": "TR",
    "columns": ["TRIAZINE"]
  },
  {
    # urea
    "label": "Urea",
    "id": "UR",
    "columns": ["UREA"]
  },
  {
    "label": "Fumigant",
    "id": "FUM",
    "columns": ["FUMIGANT", "FUMIGANT_ALL"]
  },
  {
    "label": "Oil",
    "id": "OIL",
    "columns": ["OIL", "OIL_ALL"]
  },
  {
    "label": "Biopesticide",
    "id": "BIO",
    "columns": ["BIOPESTICIDE", "BIOPESTICIDE_ALL"]
  },
  {
    # Bacillus thuringiensis BT
    "label": "Bacillus Thuringiensis",
    "id": "BT",
    "columns": ["BT", "BT_ALL"]
  },
  {
    "label": "Insect Growth Regulator",
    "id": "IGR",
    "columns": ["INSECT_GROWTH_REGULATOR", "IGR_ALL"]
  }
]


pur_meta_files = [
  {
    "file": "chemical.txt",
    "name": "chemicals",
    "calpip_col": "chem_code",
    "id_col": "chem_code",
    "out_cols": ["chem_code", "chemname"],
    "rename_cols": {
      "chemname": "chem_name"
    },
    "outpath": path.join(data_dir, "meta", "chemicals.parquet"),
    "sort_col": "chem_name",
  },
  {
    "file": "site.txt",
    "name": "sites",
    "calpip_col": "site_code",
    "id_col": "site_code",
    "outpath": path.join(data_dir, "meta", "sites.parquet"),
    "sort_col": "site_name"
  },
  {
    "file": "product.txt",
    "name": "products",
    "calpip_col": "prodno",
    "id_col": "prodno",
    "out_cols": ["prodno", "product_name"],
    "rename_cols": {
      "prodno": "product_code"
    },
    "outpath": path.join(data_dir, "meta", "products.parquet"),
    "sort_col": "product_name"
  }
]

use_stats_config = [
  {
    "data": calpip_data,
    "id_col": "chem_code",
    "use_col": "lbs_chm_used",
    "month_col": "MONTH",
    "year_col": "YEAR",
    "monthyear_col": "monthyear",
    "join_df": read_pur_meta(pur_meta_files[0]),
    "join_df_id_col": "chem_code",
    "name": "chemical_use",
    "drop_cols": ['years_used', 'months_used'],
    "outpath": pur_meta_files[0]["outpath"]
  },
  {
    "data": calpip_data,
    "id_col": "site_code",
    "use_col": "lbs_chm_used",
    "month_col": "MONTH",
    "year_col": "YEAR",
    "monthyear_col": "monthyear",
    "join_df": read_pur_meta(pur_meta_files[1]),
    "join_df_id_col": "site_code",
    "name": "site_use",
    "drop_cols": ['years_used', 'months_used'],
    "outpath": pur_meta_files[1]["outpath"]
  },
  {
    "data": calpip_data,
    "id_col": "prodno",
    "use_col": "lbs_chm_used",
    "month_col": "MONTH",
    "year_col": "YEAR",
    "monthyear_col": "monthyear",
    "join_df": read_pur_meta(pur_meta_files[2]),
    "join_df_id_col": "product_code",
    "name": "product_use",
    "drop_cols": ['years_used', 'months_used'],
    "outpath": pur_meta_files[2]["outpath"]
  }
]


# %%
calpip_data = pd.read_parquet(path.join(data_dir, "calpip", "calpip_grouped.parquet"))
calpip_data['YEAR'] = calpip_data['monthyear'].str.slice(0, 4)
calpip_data['MONTH'] = calpip_data['monthyear'].str.slice(4, 6)


def read_pur_meta(config):
  df = pd.read_csv(path.join(pur_meta_dir, config['file']), encoding='latin1')
  df[config['id_col']] = df[config['id_col']].astype(str)
  if ONLY_USE_ACTIVE:
    valid_ids = calpip_data[config['calpip_col']].unique()
    if ONLY_USE_ACTIVE:
      df = df[df[config['id_col']].isin(valid_ids)]

  if 'out_cols' in config:
    df = df[config['out_cols']]
  if 'rename_cols' in config:
    df = df.rename(columns=config['rename_cols'])
  if 'sort_col' in config:
    df = df.sort_values(by=[config['sort_col']])
  return df

def get_use_stats(data, id_col, use_col, month_col, year_col, monthyear_col, join_df, join_df_id_col, **kwargs):
  df = data[[id_col, use_col, month_col, year_col, monthyear_col]].copy()
  df[use_col] = pd.to_numeric(df[use_col], errors='coerce')

  total = df[[id_col, use_col]].groupby([id_col]).sum().reset_index()[[id_col, use_col]]
  year_average = df[[id_col, year_col, use_col]].groupby([id_col, year_col]).sum().reset_index()[[id_col, use_col]]
  year_average = year_average.groupby(id_col).mean().reset_index()
  year_average.columns = [id_col, "yearly_average"]
  year_used = df[[id_col, year_col]].groupby([id_col])[year_col].unique().reset_index()
  year_used.columns = [id_col, "years_used"]

  month_average = df[[id_col, monthyear_col, use_col]].groupby([id_col, monthyear_col]).sum().reset_index()[[id_col, use_col]]
  month_average = month_average.groupby(id_col).mean().reset_index()
  month_average.columns = [id_col, "monthly_average"]
  month_used = df[[id_col, monthyear_col]].groupby([id_col])[monthyear_col].unique().reset_index()
  month_used.columns = [id_col, "months_used"]
  monthly_high = df[[id_col, monthyear_col, use_col]].groupby([id_col, monthyear_col]).sum().reset_index()[[id_col, use_col]]
  monthly_high = monthly_high.groupby(id_col).max().reset_index()
  monthly_high.columns = [id_col, "monthly_high"]

  merged = total.merge(
    year_average,
    left_on=id_col,
    right_on=id_col
  ).merge(
    monthly_high,
    left_on=id_col,
    right_on=id_col
  ).merge(
    month_average,
    left_on=id_col,
    right_on=id_col
  ).merge(
    year_used,
    left_on=id_col,
    right_on=id_col
  ).merge(
    month_used,
    left_on=id_col,
    right_on=id_col
  )
  return join_df.merge(
    merged,
    left_on=join_df_id_col,
    right_on=id_col
  )


for config in use_stats_config:
  df = get_use_stats(**config)
  id_col = config['id_col']
  df[id_col] = df[id_col].astype(str)
  df = df.drop(columns=config['drop_cols'])
  df.to_parquet(config['outpath'])

# %%
categories_data = pd.read_excel(path.join(data_dir, "pur", '2022_meta', "AI Cat Data.xlsx"))
categories_data['CHEM_CODE'] = categories_data['CHEM_CODE'].astype(str)

rename_dict = {
  "CHEM_CODE": "chem_code",
  "AI_CLASS": "ai_class",
  "AI_TYPE": "ai_type",
  "RISK": "risk",
}

categories_data = categories_data.rename(columns=rename_dict)

if ONLY_USE_ACTIVE:
  active_chems = calpip_data['chem_code'].unique()
  categories_data = categories_data[categories_data['chem_code'].isin(active_chems)]

def condense_to_row_id(df, col) -> pd.DataFrame:

  return df[col].drop_duplicates().reset_index().rename(columns={"index": f"{col}_ID"})

class_codes = condense_to_row_id(categories_data, "ai_class")
type_codes = condense_to_row_id(categories_data, "ai_type")

# health

def apply_label(labels):
  def inner_apply_label(row):
    value = ''
    for l in labels:
      for col in l['columns']:
        if row[col] == "Y":
          value += l['id']
          value += '|' 
          break
    return value[:-1]
  return inner_apply_label

def apply_count(id):
  def inner_apply_count(row):
    label = row[id]
    if label is not None:
      return len(label.split("|"))
    return 0
  return inner_apply_count

categories_data['health_ID'] = categories_data.apply(apply_label(health_labels), axis=1)
categories_data['health_count'] = categories_data.apply(apply_count("health_ID"), axis=1)
categories_data["major_category_ID"] = categories_data.apply(apply_label(category_labels), axis=1)
categories_data['major_category_count'] = categories_data.apply(apply_count("major_category_ID"), axis=1)

categories_data = categories_data.merge(class_codes, left_on="ai_class", right_on="ai_class")
categories_data = categories_data.merge(type_codes, left_on="ai_type", right_on="ai_type")

health_labels = pd.DataFrame([{"label": l['label'], "id": l['id']} for l in health_labels])
major_category_labels = pd.DataFrame([{"label": l['label'], "id": l['id']} for l in category_labels])

raw_out_cols = [
  'chem_code',
  'ai_class_ID',
  'ai_type_ID',
  'health_ID',
  "major_category_ID",
  'risk'
]

out_cols = [col.replace("_ID", "") for col in raw_out_cols]

categories_min = categories_data[raw_out_cols].copy()

for col in raw_out_cols:
  categories_min[col] = categories_min[col].astype(str)

categories_min.columns = out_cols
# OUTPUT
categories_min.to_parquet(path.join(data_dir, "meta", "categories.parquet"))
# title case
class_codes['ai_class'] = class_codes['ai_class'].astype(str).str.title()
class_codes.to_parquet(path.join(data_dir, "meta", "class_codes.parquet"))
type_codes['ai_type'] = type_codes['ai_type'].astype(str).str.title()
type_codes.to_parquet(path.join(data_dir, "meta", "type_codes.parquet"))
health_labels.to_parquet(path.join(data_dir, "meta", "health_labels.parquet"))
major_category_labels.to_parquet(path.join(data_dir, "meta", "major_category_labels.parquet"))
pd.DataFrame(['HIGH', 'LOW', 'OTHER']).rename(columns={0: "RISK"})\
  .to_parquet(path.join(data_dir, "meta", "risk_codes.parquet"))
# %%
