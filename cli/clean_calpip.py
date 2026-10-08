# CalPIP year exports -> data/calpip/calpip_full.parquet (one row per application x active ingredient).
#   1. each data/calpip/*.zip (tab-separated CalPIP export) -> data/calpip/calpip_{year}.parquet, raw text
#   2. every calpip_{year}.parquet -> calpip_full.parquet, cleaned and typed
# Older calpip_{year}.parquet files came from the previous pandas version (missing values as 'nan', codes like
# '3551.0'); the cleaning below handles both. DuckDB streams, so memory stays flat as years are added.
import zipfile
from pathlib import Path
import duckdb

CALPIP_DIR = Path(__file__).resolve().parent.parent / "data" / "calpip"
MONTHS = "['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']"
REQUIRED = [
  "YEAR", "DATE", "ADJUVANT", "COMTRS", "AERIAL_GROUND_INDICATOR", "AG_NONAG", "AMOUNT_PLANTED", "CHEMICAL_CODE",
  "COUNTY_CODE", "POUNDS_CHEMICAL_APPLIED", "POUNDS_PRODUCT_APPLIED", "PRODUCT_CHEMICAL_PERCENT", "PRODUCT_NUMBER",
  "SITE_CODE", "USE_NUMBER",
]


def num(col):  # blank, 'nan' or junk ('MARCH', 'ACRES') -> 0
  return f"coalesce(nullif(try_cast({col} as double), 'nan'::double), 0)"


def code(col):  # '3551' or '3551.0' -> '3551'; blank or junk ('GA') -> '0'
  return f"coalesce(try_cast(trunc(nullif(try_cast({col} as double), 'nan'::double)) as bigint), 0)::varchar"


def text(col):
  return f"nullif({col}, 'nan')"


def convert_zips(con):
  done = {}
  # oldest upload first, so if two zips cover the same year the newest one wins (aws s3 sync keeps upload times)
  for zip_path in sorted(CALPIP_DIR.glob("*.zip"), key=lambda p: p.stat().st_mtime):
    print("Converting", zip_path.name)
    with zipfile.ZipFile(zip_path) as z:
      txt = next((n for n in z.namelist() if n.lower().endswith(".txt")), None)
      if not txt:
        raise ValueError(f"{zip_path.name} has no .txt file inside. Upload the zip exactly as CalPIP sent it.")
      txt_path = Path(z.extract(txt, CALPIP_DIR / "unzipped"))
    src = f"read_csv('{txt_path}', delim='\t', header=true, all_varchar=true)"
    # zips are uploaded by hand: fail with a readable message if this isn't a full, unsummarized CalPIP export
    missing = sorted(set(REQUIRED) - {c[0] for c in con.sql(f"describe select * from {src}").fetchall()})
    if missing:
      raise ValueError(f"{zip_path.name} is missing CalPIP columns {missing}. Re-request it with all output columns "
                       "and 'Summarize the data' unchecked.")
    year = con.sql(f"select mode(try_cast(try_cast(YEAR as double) as int)) from {src}").fetchone()[0]
    if not year:
      raise ValueError(f"{zip_path.name}: no YEAR")
    if year in done:
      print(f"  {year} is in both {done[year]} and {zip_path.name}; using {zip_path.name}, the newer upload")
    done[year] = zip_path.name
    # a zip replaces any existing parquet for its year (a re-pull from CalPIP)
    con.sql(f"copy (select * from {src}) to '{CALPIP_DIR}/calpip_{year}.parquet' (compression zstd)")
    txt_path.unlink()


def build_full(con):
  # meridian + township + range come straight from COMTRS (CCMTTDRRDSS, e.g. 34M03N03E01 -> MDM T03N R03E)
  con.sql(f"""copy (select
      {text('ADJUVANT')} adjuvant,
      {text('COMTRS')} comtrs,
      {num('POUNDS_CHEMICAL_APPLIED')} lbs_chm_used,
      {text('AERIAL_GROUND_INDICATOR')} aerial_ground,
      {text('AG_NONAG')} usetype,
      {num('AMOUNT_PLANTED')} amount_planted,
      {code('CHEMICAL_CODE')} chem_code,
      {code('COUNTY_CODE')} county_cd,
      {num('POUNDS_PRODUCT_APPLIED')} lbs_prd_used,
      {num('PRODUCT_CHEMICAL_PERCENT')} prodchem_pct,
      {code('PRODUCT_NUMBER')} prodno,
      {code('SITE_CODE')} site_code,
      {text('USE_NUMBER')} use_number,
      case when length(COMTRS) >= 9 then
        case COMTRS[3] when 'H' then 'HM' when 'M' then 'MDM' when 'S' then 'SBM' end
        || ' T' || COMTRS[4:6] || ' R' || COMTRS[7:9] end MeridianTownshipRange,
      '20' || DATE[-2:] || '-' || lpad(list_position({MONTHS}, DATE[4:6])::varchar, 2, '0') monthyear
    from read_parquet('{CALPIP_DIR}/calpip_20*.parquet', union_by_name=true)
    where {text('DATE')} is not null
  ) to '{CALPIP_DIR}/calpip_full.parquet' (compression zstd)""")


def main():
  con = duckdb.connect()
  con.sql("set preserve_insertion_order = false")
  convert_zips(con)
  build_full(con)


if __name__ == "__main__":
  main()
