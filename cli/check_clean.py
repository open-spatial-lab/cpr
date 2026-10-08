# Runs the clean step on tiny fake CalPIP exports and checks the output. No data needed, under a second.
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


row = dict(YEAR="2024", DATE="27-FEB-24", COMTRS="34M03N03E01", AG_NONAG="AG", CHEMICAL_CODE="3551", COUNTY_CODE="34",
           POUNDS_CHEMICAL_APPLIED="2.5", POUNDS_PRODUCT_APPLIED="10", AMOUNT_PLANTED="ACRES", PRODUCT_NUMBER="",
           SITE_CODE="GA", USE_NUMBER="7")

with tempfile.TemporaryDirectory() as d:
  cc.CALPIP_DIR = Path(d)
  con = duckdb.connect()
  write_zip(Path(d, "old.zip"), [row], mtime=1)
  # same year uploaded later wins; the entry name has a quote, which must not reach the SQL
  write_zip(Path(d, "new.zip"), [{**row, "POUNDS_CHEMICAL_APPLIED": "3"}], mtime=2, member="it's 2024.txt")
  # a line break inside a field splits that record in two (the 2021 export has one); it must not crash
  write_zip(Path(d, "split.zip"), [{**row, "YEAR": "2025", "DATE": "01-MAR-25", "ADJUVANT": "SPLIT\nRECORD"}], mtime=3)
  # years left by the old pandas version: 'nan' for missing, codes as floats; HM and SBM meridians
  for year, date, comtrs in [("2017", "01-DEC-17", "27S12S12E05"), ("2018", "15-JUN-18", "12H05N01E10")]:
    con.sql(f"""copy (select '{year}' AS YEAR, '{date}' AS DATE, '{comtrs}' AS COMTRS, '3551.0' AS CHEMICAL_CODE,
      'nan' AS POUNDS_CHEMICAL_APPLIED, 'nan' AS AG_NONAG, '1.0' AS COUNTY_CODE, '5' AS POUNDS_PRODUCT_APPLIED)
      to '{d}/calpip_{year}.parquet'""")
  cc.convert_zips(con)
  cc.build_full(con)
  got = {r[0][:4]: r for r in con.sql(f"""select monthyear, chem_code, county_cd, lbs_chm_used, lbs_prd_used, amount_planted, prodno,
    site_code, usetype, MeridianTownshipRange from '{d}/calpip_full.parquet' where monthyear is not null""").fetchall()}
  assert got == {
    "2024": ("2024-02", "3551", "34", 3.0, 10.0, 0.0, "0", "0", "AG", "MDM T03N R03E"),
    # first half of the split record: only YEAR, DATE, ADJUVANT survive, the rest clean to 0 (as pandas did)
    "2025": ("2025-03", "0", "0", 0.0, 0.0, 0.0, "0", "0", None, None),
    "2017": ("2017-12", "3551", "1", 0.0, 5.0, 0.0, "0", "0", None, "SBM T12S R12E"),
    "2018": ("2018-06", "3551", "1", 0.0, 5.0, 0.0, "0", "0", None, "HM T05N R01E"),
  }, got
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
