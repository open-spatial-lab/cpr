# %%
import pandas as pd
from pathlib  import Path
import os

ONLY_USE_ACTIVE = os.getenv("ONLY_USE_ACTIVE", "true") == "true"
DATA_DIR = Path(__file__).parent.parent / "data"
pur_meta_dir = DATA_DIR / "pur" / "meta"

HEALTH_LABELS = [
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
    "label": "Reproductive Toxin",
    "id": "REPR",
    "columns": ["REPRODUCTIVE", "REPRODUCTIVE_ALL"]
  },
  {
    "label": "Toxic Air Contaminant",
    "id": "TAC",
    "columns": ["TAC", "TAC_ALL"]
  },
  {
    "label": "Cholinesterase Inhibitor",
    "id": "CHI",
    "columns": ["CARBAMATE", "CARB_ALL","OP", "OP_ALL"]
  },
  {
    "label": "Restricted Material",
    "id": "RM",
    "columns": ["Restricted Material"]
  }
]

CATEGORY_LABELS = [
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
  # {
  #   # Bacillus thuringiensis BT
  #   "label": "Bacillus Thuringiensis",
  #   "id": "BT",
  #   "columns": ["BT", "BT_ALL"]
  # },
  {
    "label": "Insect Growth Regulator",
    "id": "IGR",
    "columns": ["INSECT_GROWTH_REGULATOR", "IGR_ALL"]
  }
]

PUR_META_FILES = [
  {
    "file": "chemical.txt",
    "name": "chemicals",
    "calpip_col": "chem_code",
    "id_col": "chem_code",
    "out_cols": ["chem_code", "chemname"],
    "rename_cols": {
      "chemname": "chem_name"
    },
    "outpath": DATA_DIR / "meta" / "chemicals.parquet",
    "sort_col": "chem_name",
  },
  {
    "file": "site.txt",
    "name": "sites",
    "calpip_col": "site_code",
    "id_col": "site_code",
    "outpath": DATA_DIR / "meta" / "sites.parquet",
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
    "outpath": DATA_DIR / "meta" / "products.parquet",
    "sort_col": "product_name"
  }
]

RENAME_DICT = {
  "CHEM_CODE": "chem_code",
  "AI_TYPE": "ai_type",
}

def get_use_stats_config(calpip_data):
  return [
    {
      "data": calpip_data,
      "id_col": "chem_code",
      "use_col": "lbs_chm_used",
      "month_col": "MONTH",
      "year_col": "YEAR",
      "monthyear_col": "monthyear",
      "join_df": read_pur_meta(PUR_META_FILES[0], calpip_data),
      "join_df_id_col": "chem_code",
      "name": "chemical_use",
      "drop_cols": ['years_used', 'months_used'],
      "outpath": PUR_META_FILES[0]["outpath"]
    },
    {
      "data": calpip_data,
      "id_col": "site_code",
      "use_col": "lbs_chm_used",
      "month_col": "MONTH",
      "year_col": "YEAR",
      "monthyear_col": "monthyear",
      "join_df": read_pur_meta(PUR_META_FILES[1], calpip_data),
      "join_df_id_col": "site_code",
      "name": "site_use",
      "drop_cols": ['years_used', 'months_used'],
      "outpath": PUR_META_FILES[1]["outpath"]
    },
    {
      "data": calpip_data,
      "id_col": "prodno",
      "use_col": "lbs_chm_used",
      "month_col": "MONTH",
      "year_col": "YEAR",
      "monthyear_col": "monthyear",
      "join_df": read_pur_meta(PUR_META_FILES[2], calpip_data),
      "join_df_id_col": "product_code",
      "name": "product_use",
      "drop_cols": ['years_used', 'months_used'],
      "outpath": PUR_META_FILES[2]["outpath"]
    }
  ]

def read_pur_meta(config, calpip_data):
  df = pd.read_csv(pur_meta_dir / config['file'], encoding='latin1')
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

def condense_to_row_id(df, col) -> pd.DataFrame:
  return df[col].drop_duplicates().reset_index().rename(columns={"index": f"{col}_ID"})

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

def main():
  calpip_data = pd.read_parquet(DATA_DIR / "calpip" / "calpip_full.parquet")
  calpip_data['YEAR'] = calpip_data['monthyear'].str.slice(0, 4)
  calpip_data['MONTH'] = calpip_data['monthyear'].str.slice(4, 6)
  use_stats_config = get_use_stats_config(calpip_data)
  for config in use_stats_config:
    df = get_use_stats(**config)
    id_col = config['id_col']
    df[id_col] = df[id_col].astype(str)
    df = df.drop(columns=config['drop_cols'])
    df.to_parquet(config['outpath'])

  categories_data = pd.read_excel(pur_meta_dir / "AI Cat Data.xlsx")
  categories_data['CHEM_CODE'] = categories_data['CHEM_CODE'].astype(str)
  restricted_materials_data = pd.read_excel(pur_meta_dir / "Restricted Pesticides.xlsx").iloc[1:]
  restricted_materials_data.columns = ['name', 'DPR chem code', 'Restricted Material', '_']
  restricted_materials_data = restricted_materials_data[['DPR chem code', 'Restricted Material']]

  restricted_materials_data['Restricted Material'] = 'Y'
  restricted_materials_data['DPR chem code'] = pd.to_numeric(restricted_materials_data['DPR chem code'], errors='coerce')
  restricted_materials_data = restricted_materials_data.dropna(subset=['DPR chem code'])

  restricted_materials_data['DPR chem code'] = restricted_materials_data['DPR chem code'].astype(int).astype(str)
  categories_data = categories_data.merge(restricted_materials_data, left_on="CHEM_CODE", right_on="DPR chem code", how="left")
  categories_data = categories_data.rename(columns=RENAME_DICT)

  if ONLY_USE_ACTIVE:
    active_chems = calpip_data['chem_code'].unique()
    categories_data = categories_data[categories_data['chem_code'].isin(active_chems)]

  categories_no_adj = categories_data[categories_data.ai_type != 'ADJUVANT']
  use_type_codes = condense_to_row_id(categories_no_adj, "ai_type")
  # health


  categories_data['health_ID'] = categories_data.apply(apply_label(HEALTH_LABELS), axis=1)
  categories_data['health_count'] = categories_data.apply(apply_count("health_ID"), axis=1)
  categories_data["major_category_ID"] = categories_data.apply(apply_label(CATEGORY_LABELS), axis=1)
  categories_data['major_category_count'] = categories_data.apply(apply_count("major_category_ID"), axis=1)

  categories_data = categories_data.merge(use_type_codes, left_on="ai_type", right_on="ai_type")

  health_labels_output = pd.DataFrame([{"label": l['label'], "id": l['id']} for l in HEALTH_LABELS])
  chemical_class_labels = pd.DataFrame([{"label": l['label'], "id": l['id']} for l in CATEGORY_LABELS])

  raw_out_cols = [
    'chem_code',
    "major_category_ID",
    'ai_type_ID',
    'health_ID',
  ]

  out_cols = [col.replace("_ID", "") for col in raw_out_cols]

  categories_min = categories_data[raw_out_cols].copy()

  for col in raw_out_cols:
    categories_min[col] = categories_min[col].astype(str)

  categories_min.columns = out_cols

  # OUTPUT
  categories_min.drop_duplicates().to_parquet(DATA_DIR / "meta" / "categories.parquet")
  use_type_codes['ai_type'] = use_type_codes['ai_type'].astype(str).str.title()
  use_type_codes.drop_duplicates().to_parquet(DATA_DIR / "meta" / "use_type_codes.parquet")
  health_labels_output.drop_duplicates().to_parquet(DATA_DIR / "meta" / "health_labels.parquet")
  chemical_class_labels.drop_duplicates().to_parquet(DATA_DIR / "meta" / "chemical_class_labels.parquet")
# %%
if __name__ == "__main__":
  main()
# %%
