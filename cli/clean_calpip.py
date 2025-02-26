# %% 
import pandas as pd
import geopandas as gpd
from glob import glob
import numpy as np
from pathlib import Path
import zipfile
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CALPIP_DIR = DATA_DIR / "calpip"
CALPIP_FILES = glob(str(CALPIP_DIR / "*.zip"))

DATE_COLS = [
  'DATE',
]
COLS_TO_KEEP = [
  'ADJUVANT', 
  # 'DATE', 
  # 'COUNTY_NAME', 
  'COMTRS', 
  # 'SITE_NAME',
  # 'PRODUCT_NAME', 
  'POUNDS_PRODUCT_APPLIED', 
  # 'CHEMICAL_NAME',
  # 'AMOUNT_TREATED', 
  # 'UNIT_TREATED',
  'AERIAL_GROUND_INDICATOR', 
  'AERIAL_GROUND_DESCRIPTION', 
  'AG_NONAG',
  'AMOUNT_PLANTED', 
  # 'AMOUNT_PRODUCT_APPLIED', 
  # 'APPLICATION_MONTH',
  'CHEMICAL_CODE', 
  'COUNTY_CODE', 
  # 'GROWER_ID', 
  'LICENSE_NUMBER',
  'OUTLIER', 
  # 'PERMITTING_COUNTY_NAME', 
  # 'PERMIT_NUMBER',
  # 'PERMIT_YEAR', 
  # 'PLANTING_SEQUENCE', 
  'PRODUCT_CHEMICAL_PERCENT',
  'PRODUCT_NUMBER', 
  # 'QUALIFY_CODE', 
  # 'REGISTRATION_NUMBER', 
  'SITE_CODE',
  'SITE_LOCATION_ID', 
  # 'UNITS_PRODUCT_APPLIED', 
  # 'UNIT_PLANTED',
  # 'UNIT_PLANTED_DESCRIPTION', 
  # 'UNIT_PRODUCT_APPLIED',
  # 'UNIT_TREATED_DESCRIPTION', 
  'USE_NUMBER'
]
SUM_COL = [
  'POUNDS_CHEMICAL_APPLIED'
]

COLUMN_MAPPING = {
  "ADJUVANT": "adjuvant",
  "DATE": "date",
  'COMTRS': "comtrs",
  'POUNDS_CHEMICAL_APPLIED':"lbs_chm_used",
  'AERIAL_GROUND_INDICATOR': "aerial_ground",
  # 'AERIAL_GROUND_DESCRIPTION', 
  'AG_NONAG': "usetype",
  'AMOUNT_PLANTED': "amount_planted",
  'CHEMICAL_CODE': "chem_code",
  'COUNTY_CODE': 'county_cd', 
  # 'GROWER_ID': "grower_id",
  # 'LICENSE_NUMBER',
  # 'OUTLIER', 
  'POUNDS_PRODUCT_APPLIED': "lbs_prd_used",
  'PRODUCT_CHEMICAL_PERCENT': "prodchem_pct",
  'PRODUCT_NUMBER': "prodno",
  'SITE_CODE': "site_code",
  "USE_NUMBER": "use_number",
  # 'SITE_LOCATION_ID', 
}

NUMERIC_COLS = [
  'lbs_chm_used',
  'amount_planted',
  'lbs_prd_used',
  'prodchem_pct'
]

NUMERIC_REPLACEMENTS = [
  'nan',
  'MARCH',
  'ACRES'
]

STRING_INT_COLS = [
  'chem_code',
  'county_cd',
  'prodno',
  'site_code',
]

STRING_INT_REPLACEMENTS = [
  "GA"
]

def clean_calpip(
    filepath,
    date_format_in='%d-%b-%y',
    date_format_out='%Y-%m',
    df=None
  ):
  # unzip filepath
  with zipfile.ZipFile(filepath, 'r') as zip_ref:
      zip_ref.extractall(filepath.replace('.zip', ''))
  try: 
    if df is None:
      filename = filepath.split('/')[-1].replace('.zip', '')
      txtpath = filepath.replace('.zip', f'/{filename}.txt')
      df = pd.read_csv(txtpath, sep="\t")
    else:
      df = df.copy()
    print('file read')

    initial_rows = df.shape[0]
    df = df[(df.DATE.notna())]
    rows_after_date_notna = df.shape[0]
    if initial_rows != rows_after_date_notna:
      print('rows dropped:', initial_rows - rows_after_date_notna)
    df['MONTHYEAR'] = pd.to_datetime(
      df['DATE'], 
      format=date_format_in,
      errors="coerce")\
        .dt.strftime(date_format_out)
    print('date conversion done')
    year = int(df[df.YEAR.notna()]['YEAR'].mode()[0])
    if year is None or year == 0:
      # error
      raise Exception('Year not found')
    df['YEAR'] = int(year)
    
    for col in df.columns:
      if col != "YEAR":
        df[col] = df[col].astype(str)
      
    print('str conversion done')
    df.to_parquet(CALPIP_DIR / f"calpip_{year}.parquet")
    print('parquet conversion done')
    with open(BASE_DIR / f"files_to_remove.txt", 'a') as f:
      f.write(f"calpip/{filename}.zip\n")
    return {
      "ok": True,
      "result": df
    }
  except Exception as e:
    print('Error:', e)
    print('file:', filepath)
    return {
      "ok": False,
      "error": e,
      "result": df
    }

def clean_parquets():
  calpip_parquets = glob(str(CALPIP_DIR / "calpip_20*.parquet"))
  dfs = []
  for file in calpip_parquets:
    print(file)
    dfs.append(pd.read_parquet(file)[COLUMN_MAPPING.keys()])
  df = pd.concat(dfs)
  df = df.rename(columns=COLUMN_MAPPING)
  return df

def clean_columns(df, numeric_cols=NUMERIC_COLS, numeric_replacements=NUMERIC_REPLACEMENTS, string_int_cols=STRING_INT_COLS, string_int_replacements=STRING_INT_REPLACEMENTS):
  df = df.copy()
  for col in numeric_cols:
    for rep in numeric_replacements:
      df.loc[df[col] == rep, col] = np.nan
    df[col] = pd.to_numeric(df[col]).fillna(0)

  for col in string_int_cols:
    for rep in string_int_replacements:
      df.loc[df[col] == rep, col] = np.nan
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df[col] = df[col].fillna(0).astype(int).astype(str).fillna('')

  return df

def convert_comtrs(comtrs):
    if len(comtrs) < 9:
      return None
    township_number = comtrs[3:5]
    township_direction = comtrs[5]
    range_number = comtrs[6:8]
    range_direction = comtrs[8]
    return f"T{township_number}{township_direction} R{range_number}{range_direction}"

# %%
def get_sections():
  townships = gpd.read_file(DATA_DIR / 'geo'/ 'PLSS Township Range California.geojson')
  meridians = townships.dissolve(by='Meridian').reset_index()
  meridians = meridians[['Meridian', 'geometry']]
  meridians = meridians.to_crs("EPSG:3310")

  sections = gpd.read_parquet(DATA_DIR / 'sections'/ 'sections.parquet')
  sections = sections.to_crs("EPSG:3310")
  sections['centroid'] = sections.centroid
  sections = sections.set_geometry('centroid')
  # sjoin to
  sections = gpd.sjoin(sections, meridians, predicate='within')
  sections = sections[['CO_MTRS', 'Meridian']]
  return sections
# %%
month_dict = {
  "JAN": '01',
  "FEB": '02',
  "MAR": '03',
  "APR": '04',
  "MAY": '05',
  "JUN": '06',
  "JUL": '07',
  "AUG": '08',
  "SEP": '09',
  "OCT": '10',
  "NOV": '11',
  "DEC": '12'
}

def clean_years(df, month_dict=month_dict):
  df['year'] = '20' + df['date'].str.slice(-2,)
  df['month'] = df['date'].str.slice(3, 6).map(month_dict)
  df['monthyear'] = df['year'] + '-' + df['month']
  return df
# %%
def main():
  for file in CALPIP_FILES:
    print('Cleaning:', file)
    clean_calpip(file)

  df = clean_parquets()
  df = clean_columns(df)
  df['TownshipRange'] = df['comtrs'].apply(convert_comtrs)
  
  sections = get_sections()
  df = df.merge(sections, left_on='comtrs', right_on='CO_MTRS', how='left')
  df['MeridianTownshipRange'] = df['Meridian'] + ' ' + df['TownshipRange']
  
  df = clean_years(df)
  to_drop = ['date', 'year', 'month', 'TownshipRange', "CO_MTRS", "Meridian"]
  
  df = df.drop(columns=to_drop)
  df.to_parquet(CALPIP_DIR / 'calpip_full.parquet', compression='gzip')

if __name__ == "__main__":
  main()