# %% 
import pandas as pd
import geopandas as gpd
from os import path
from glob import glob
import numpy as np
# remove col limimt df
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# %%
current_dir = path.dirname(__file__)
calpip_dir = path.join(current_dir, "..", "data", "calpip")
# %%
calpip_files = glob(path.join(calpip_dir, "raw", "*.txt"))

# %%
date_cols = [
  'DATE',
]
cols_to_keep = [
  # 'ADJUVANT', 
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
  # 'USE_NUMBER'
]
sum_col = [
  'POUNDS_CHEMICAL_APPLIED'
]
# %%
def clean_calpip(
    filepath,
    date_format_in='%d-%b-%y',
    date_format_out='%Y-%m',
    df=None
  ):
  try: 
    if df is None:
      df = pd.read_csv(filepath, sep="\t")
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
    df.to_parquet(path.join(calpip_dir, f"calpip_{year}.parquet"))
    print('parquet conversion done')
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
# %%
dfs = []
for file in calpip_files:
  print('Cleaning:', file)
  r = clean_calpip(file)
  if not r['ok']:
    print(r['error'])
  else:
    dfs.append(r['result'])  

# %%
column_mapping = {
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
  'SITE_CODE': "site_code"
  # 'SITE_LOCATION_ID', 
}

should_be_numbers = [
  'lbs_chm_used',
  'amount_planted',
  'lbs_prd_used',
  'prodchem_pct'
]

calpip_parquets = glob(path.join(calpip_dir,  "calpip_20*.parquet"))
for file in calpip_parquets:
  df = pd.read_parquet(file)
  print(file)
df = pd.concat([pd.read_parquet(file)[column_mapping.keys()] for file in calpip_parquets])
df = df.rename(columns=column_mapping)
# %%
replacements = [
  'nan',
  'MARCH',
  'ACRES'
]
for col in should_be_numbers:
  for rep in replacements:
    df[col] = df[col].replace(rep, np.nan)
  df[col] = pd.to_numeric(df[col]).fillna(0)
# %%
should_be_string_ints = [
  'chem_code',
  'county_cd',
  'prodno',
  'site_code',
]

replacements = [
  "GA"
]
for col in should_be_string_ints:
  for rep in replacements:
    df[col] = df[col].replace(rep, np.nan)
  df[col] = pd.to_numeric(df[col])
  df[col] = df[col].fillna(0).astype(int).astype(str).fillna('')
# %%
def convert_comtrs(comtrs):
    if len(comtrs) < 9:
      return None
    township_number = comtrs[3:5]
    township_direction = comtrs[5]
    range_number = comtrs[6:8]
    range_direction = comtrs[8]
    return f"T{township_number}{township_direction} R{range_number}{range_direction}"

df['TownshipRange'] = df['comtrs'].apply(convert_comtrs)
# %%
townships = gpd.read_file('../data/geo/PLSS Township Range California.geojson')
meridians = townships.dissolve(by='Meridian').reset_index()
meridians = meridians[['Meridian', 'geometry']]
meridians = meridians.to_crs("EPSG:3310")

sections = gpd.read_parquet('../data/sections/sections.parquet')
sections = sections.to_crs("EPSG:3310")
sections['centroid'] = sections.centroid
sections = sections.set_geometry('centroid')
# sjoin to
sections = gpd.sjoin(sections, meridians, predicate='within')
sections = sections[['CO_MTRS', 'Meridian']]

df = df.merge(sections, left_on='comtrs', right_on='CO_MTRS', how='left')
df['MeridianTownshipRange'] = df['Meridian'] + ' ' + df['TownshipRange']
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
df['year'] = '20' + df['date'].str.slice(-2,)
df['month'] = df['date'].str.slice(3, 6).map(month_dict)
df['monthyear'] = df['year'] + '-' + df['month']
# %%
to_drop = ['date', 'year', 'month', 'TownshipRange', "CO_MTRS", "Meridian"]
df = df.drop(columns=to_drop)
# %%
df.to_parquet(path.join(calpip_dir, 'calpip_full.parquet'), compression='gzip')
# %%
# check data
df_state = "finished"
sf_chem_code = "618"
cf_site_code = "90"
sum_col = "lbs_chm_used"
usetype_col = "usetype"

if df_state == "rawraw":
  df = pd.concat(dfs)
  sum_col = "POUNDS_CHEMICAL_APPLIED"
  usetype_col = "AG_NONAG"
  chem_filter = df.CHEMICAL_CODE == float(sf_chem_code)
  site_filter = df.SITE_CODE == float(cf_site_code)
  year_filter = df.DATE.str.endswith('20')|df.DATE.str.endswith('21')
elif df_state == "parquets":
  sum_col = "POUNDS_CHEMICAL_APPLIED"
  usetype_col = "AG_NONAG"
  df20 = pd.read_parquet(path.join(calpip_dir, 'calpip_2020.parquet'))
  df21 = pd.read_parquet(path.join(calpip_dir, 'calpip_2021.parquet'))
  df = pd.concat([df20, df21])
  chem_filter = df.CHEMICAL_CODE == float(sf_chem_code)
  site_filter = df.SITE_CODE == float(cf_site_code)
  year_filter = df.MONTHYEAR.str.startswith('2020')|df.MONTHYEAR.str.startswith('2021')
else:
  chem_filter = df.chem_code == sf_chem_code
  site_filter = df.site_code == cf_site_code
  year_filter = df.monthyear.str.startswith('2020')|df.monthyear.str.startswith('2021')

filtered = df[chem_filter&site_filter&year_filter]
# %%
# filtered_debug = filtered.copy()

# date_format_in='%d-%b-%y'
# date_format_out='%Y-%m'
# filtered_debug['datetime'] =  pd.to_datetime(
#       filtered_debug['DATE'], 
#       format=date_format_in,
#       errors="coerce")\
#         .dt.strftime(date_format_out)

# year = int(filtered_debug['YEAR'].mode())
# filtered_debug['YEAR'] = year
# str_cols = [
#   'COMTRS',
#   'GROWER_ID',
#   'LICENSE_NUMBER',
#   'PERMIT_NUMBER',
#   'SITE_LOCATION_ID',
# ]
# for col in str_cols:
#   filtered_debug[col] = filtered_debug[col].astype(str)

# filtered_debug.to_parquet("test.parquet")
# filtered_debug = pd.read_parquet("test.parquet")
# %%
# %%
out_df = filtered

ag = out_df[out_df[usetype_col] == 'AG']
nonag = out_df[out_df[usetype_col] == 'NON-AG']
# %5

print(f"sum ag {ag[sum_col].sum()}")
print(f"sum nonag {nonag[sum_col].sum()}")
# %%
group_cols = [
  'comtrs', 
  'aerial_ground', 
  'usetype', 
  'chem_code', 
  'county_cd', 
  'prodchem_pct',
  'prodno', 
  'site_code', 
  'MeridianTownshipRange', 
  'monthyear' 
]
for col in group_cols:
  df[col] = df[col].fillna('')

grouped  = df.groupby(group_cols).sum().reset_index()
# %%
grouped.to_parquet(path.join(calpip_dir, 'calpip_grouped.parquet'), compression='gzip')
# %%