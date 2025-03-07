# %%
import geopandas as gpd
from pathlib import Path
CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR.parent / 'data'
cal_albers = 'EPSG:3310'
min_area = .05

def do_intersection(source, target, source_id, target_id, outpath):
  target = target.to_crs(cal_albers)
  target['TARGET_AREA'] = target.area

  source = source.to_crs(cal_albers)
  source['SOURCE_AREA'] = source.area

  output_cols = [source_id, target_id, 'AREA_RATIO']

  intersected = gpd.overlay(target, source, how='intersection')
  intersected = intersected.dissolve(by=[source_id, target_id]).reset_index()
  intersected['NEW_AREA'] = intersected.area
  intersected['AREA_RATIO'] = intersected['NEW_AREA'] / intersected['SOURCE_AREA']
  intersected['AREA_RATIO'] = intersected['AREA_RATIO'].round(4)
  intersected['GREATER_THAN_MIN_AREA'] = intersected['AREA_RATIO'] > (1 - min_area)
  greater_than_min_ids = intersected[intersected['GREATER_THAN_MIN_AREA']][source_id].unique()
  intersected = intersected[(intersected['GREATER_THAN_MIN_AREA'])|(~intersected[source_id].isin(greater_than_min_ids))]
  intersected = intersected[output_cols]

  summed = intersected[[source_id, "AREA_RATIO"]].groupby(source_id).sum().reset_index()
  merged  = intersected.merge(summed, on=source_id, suffixes=('', '_sum'))
  
  corrected = merged.copy()
  corrected['AREA_RATIO'] = corrected['AREA_RATIO'] * (1 / corrected['AREA_RATIO_sum'])
  corrected = corrected[output_cols]

  corrected.to_parquet(outpath, compression='gzip')
  return corrected
# %%
formats = [
  {
    "census_path": DATA_DIR / 'census_geos' / 'ca-zip.parquet',
    "id_col": "ZCTA5CE20",
    "outpath": DATA_DIR / 'census_geos' / 'crosswalks' / 'zip_intersections.parquet' 
  },
  {
    "census_path": DATA_DIR / "census_geos" / "ca-tract.parquet",
    "id_col": "GEOID",
    "outpath": DATA_DIR / 'census_geos' / 'crosswalks'/'tract_intersections.parquet'
  },
  {
    "census_path": DATA_DIR / "census_geos" / "ca-school.parquet",
    "id_col": "FIPS",
    "outpath": DATA_DIR / 'census_geos' / 'crosswalks'/'school_district_intersections.parquet'
  }
]

# %%
def main():
  sections = gpd.read_parquet(DATA_DIR /'sections' / 'sections.parquet')
  sections = sections.to_crs(cal_albers)

  for format in formats:
    census_geo = gpd.read_parquet(format['census_path'])
    intersected = do_intersection(sections, census_geo, "CO_MTRS", format['id_col'], format['outpath'])

  tracts = gpd.read_parquet(DATA_DIR / 'census_geos' / 'ca-tract.parquet')

  do_intersection(
    tracts, 
    sections,
    'GEOID',
    "CO_MTRS",
    DATA_DIR / 'census_geos' / 'crosswalks' / 'sections-to-tracts.parquet', 
  )
  townships = gpd.read_file(DATA_DIR / 'geo' / 'CA-townships-2023.geojson')
  townships['MeridianTownshipRange'] = townships['Meridian'] + " " +  townships['TownshipRange']
  townships['MeridianTownshipRange'] = townships['MeridianTownshipRange'].str.strip()
  townships = townships[townships['MeridianTownshipRange'] != '']
  # dissolve geo on MeridianTownshipRange
  townships = townships.dissolve(by='MeridianTownshipRange').reset_index()
  
  ts_int = do_intersection(
    tracts, 
    townships, 
    'GEOID',
    "MeridianTownshipRange",
    DATA_DIR /'census_geos' / 'crosswalks' / 'townships-to-tracts.parquet', 
  )
if __name__ == "__main__":
  main()