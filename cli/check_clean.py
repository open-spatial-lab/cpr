# Runs the clean step on tiny fake CalPIP exports and checks every output row and column. No data needed, under a second.
#   python3 cli/check_clean.py
import os
import sys
import tempfile
import zipfile
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).parent))
import clean_calpip as cc
from compare_builds import change


def write_zip(path, rows, cols=cc.REQUIRED, mtime=None, member="export.txt"):
  lines = ["\t".join(cols)] + ["\t".join(r.get(c, "") for c in cols) for r in rows]
  with zipfile.ZipFile(path, "w") as z:
    z.writestr(member, "\n".join(lines) + "\n")
  if mtime:
    os.utime(path, (mtime, mtime))


row = dict(YEAR="2024", DATE="27-FEB-24", COMTRS="34M03N03E01", AG_NONAG="AG", AERIAL_GROUND_INDICATOR="G",
           CHEMICAL_CODE="3551", COUNTY_CODE="34", POUNDS_CHEMICAL_APPLIED="2.5", POUNDS_PRODUCT_APPLIED="10",
           PRODUCT_CHEMICAL_PERCENT="25.5", AMOUNT_PLANTED="ACRES", PRODUCT_NUMBER="", SITE_CODE="GA", USE_NUMBER="7")

with tempfile.TemporaryDirectory() as d:
  cc.CALPIP_DIR = Path(d)
  con = duckdb.connect()
  write_zip(Path(d, "old.zip"), [row], mtime=1)
  # same year uploaded later wins; its entry name has a quote, which must not reach the SQL; a blank DATE is dropped
  write_zip(Path(d, "new.zip"), [{**row, "POUNDS_CHEMICAL_APPLIED": "3"}, {**row, "DATE": "", "USE_NUMBER": "8"}],
            mtime=2, member="it's 2024.txt")
  # a line break inside a field splits that record in two (the 2021 export has one). It must not crash; the first
  # half keeps its date, the shifted second half has no parseable month and is dropped
  write_zip(Path(d, "split.zip"), [{**row, "YEAR": "2025", "DATE": "01-MAR-25", "ADJUVANT": "SPLIT\nRECORD"}], mtime=3)
  # years left by the old pandas version: 'nan' for missing, codes as floats; HM and SBM meridians; a 'nan' DATE
  for year, date, comtrs in [("2017", "01-DEC-17", "27S12S12E05"), ("2018", "15-JUN-18", "12H05N01E10")]:
    con.sql(f"""copy (select * from (values ('{year}', '{date}', '{comtrs}'), ('{year}', 'nan', '{comtrs}'))
        t(YEAR, DATE, COMTRS), (select '3551.0' AS CHEMICAL_CODE, 'nan' AS POUNDS_CHEMICAL_APPLIED, 'nan' AS AG_NONAG,
        '1.0' AS COUNTY_CODE, '5' AS POUNDS_PRODUCT_APPLIED, 'nan' AS USE_NUMBER))
      to '{d}/calpip_{year}.parquet'""")
  cc.convert_zips(con)
  cc.build_full(con)
  got = con.sql(f"select * from '{d}/calpip_full.parquet' order by monthyear").fetchall()
  # adjuvant, comtrs, lbs_chm_used, aerial_ground, usetype, amount_planted, chem_code, county_cd, lbs_prd_used,
  # prodchem_pct, prodno, site_code, use_number, MeridianTownshipRange, monthyear
  assert got == [
    (None, "27S12S12E05", 0.0, None, None, 0.0, "3551", "1", 5.0, 0.0, "0", "0", None, "SBM T12S R12E", "2017-12"),
    (None, "12H05N01E10", 0.0, None, None, 0.0, "3551", "1", 5.0, 0.0, "0", "0", None, "HM T05N R01E", "2018-06"),
    (None, "34M03N03E01", 3.0, "G", "AG", 0.0, "3551", "34", 10.0, 25.5, "0", "0", "7", "MDM T03N R03E", "2024-02"),
    ("SPLIT", None, 0.0, None, None, 0.0, "0", "0", 0.0, 0.0, "0", "0", None, None, "2025-03"),
  ], got
  # a summarized export is rejected with the missing columns named
  write_zip(Path(d, "summarized.zip"), [row], cols=[c for c in cc.REQUIRED if c != "USE_NUMBER"])
  try:
    cc.convert_zips(con)
    raise AssertionError("summarized export accepted")
  except ValueError as e:
    assert "USE_NUMBER" in str(e), e

assert change(103.0, 100.0) == "+3.00%" and change(100.0, 100.0000001) == "0.00%"
assert change(5.0, None) == "new year" and change(None, 5.0) == "**removed**"
print("clean ok")
