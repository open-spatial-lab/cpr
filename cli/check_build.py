# Runs build_r2 on a tiny synthetic dataset and checks the numbers behind the site. No data needed, under a second.
#   python3 cli/check_build.py
import json
import sys
import tempfile
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).parent))
import build_r2

with tempfile.TemporaryDirectory() as tmp:
  d = Path(tmp)
  for sub in ("calpip", "census_data", "census_geos/crosswalks", "output", "meta"):
    (d / sub).mkdir(parents=True)
  con = duckdb.connect()
  sec, twp = "01M01N01E01", "MDM T01N R01E"
  # use 1 (2023) and use 2 share a cell, each with two AI rows; use 3 is non-ag (on a section, so a missing ag
  # filter would show); use 1 (2022) matches use 1 (2023) in everything but the year, so the year must be in the key
  con.sql(f"""copy (select * from (values
      ('1', '1', '{sec}', '{twp}', '2023-05', 'AG', 'G', '100', '5', 'a', 2.0, 10.0),
      ('1', '1', '{sec}', '{twp}', '2023-05', 'AG', 'G', '100', '5', 'b', 3.0, 10.0),
      ('2', '1', '{sec}', '{twp}', '2023-05', 'AG', 'G', '100', '5', 'a', 1.0, 4.0),
      ('2', '1', '{sec}', '{twp}', '2023-05', 'AG', 'G', '100', '5', 'b', 1.0, 4.0),
      ('3', '1', '{sec}', '{twp}', '2023-06', 'NON-AG', null, '200', '7', 'a', 7.0, 7.0),
      ('1', '1', '{sec}', '{twp}', '2022-01', 'AG', 'A', '100', '6', 'a', 5.0, 10.0))
    t(use_number, county_cd, comtrs, MeridianTownshipRange, monthyear, usetype, aerial_ground, site_code, prodno,
      chem_code, lbs_chm_used, lbs_prd_used)) to '{d}/calpip/calpip_full.parquet'""")
  (d / "census_data/ca-county-dpr-xwalk.csv").write_text("FIPS,Area Name,DPR_ID\n06001,Alameda County,01\n")
  for file, col, rows in [("tract_intersections", "GEOID", "('t1', 0.25), ('t2', 0.75)"),
                          ("school_district_intersections", "FIPS", "('s1', 1.0)"), ("zip_intersections", "ZCTA5CE20", "('z1', 1.0)")]:
    con.sql(f"""copy (select '{sec}' AS CO_MTRS, * from (values {rows}) t({col}, AREA_RATIO))
      to '{d}/census_geos/crosswalks/{file}.parquet'""")
  demog = ", ".join(f'1 AS "{c}"' for c in build_r2.DEMOG_COLS)  # integers, like the real files
  for name, key, value, has_name in [("county", "FIPS", "06001", True), ("section", "comtrs", sec, False),
                                     ("township", "MeridianTownshipRange", twp, False), ("tract", "GEOID", "t1", True),
                                     ("school", "FIPS", "s1", True), ("zip", "GEOID", "z1", False)]:
    area_name = "'Somewhere' AS \"Area Name\", " if has_name else ""
    con.sql(f"""copy (select '{value}' AS "{key}", {area_name}2589988.110336 AS ALAND, {demog})
      to '{d}/output/ca-{name}-demography.parquet'""")
  for name, q in [("chemicals", "'a' AS chem_code, 'A' AS chem_name, 1.0 AS yearly_average"),
                  ("sites", "'100' AS site_code, 'S' AS site_name, 1.0 AS yearly_average"),
                  ("products", "'5' AS product_code, 'P' AS product_name, 1.0 AS yearly_average"),
                  ("chemical_class_labels", "'Class' AS label, 'C' AS id"), ("health_labels", "'Health' AS label, 'H' AS id"),
                  ("use_type_codes", "0 AS ai_type_ID, 'Type' AS ai_type"),
                  ("categories", "'a' AS chem_code, 'C' AS major_category, '0' AS ai_type, 'H' AS health")]:
    con.sql(f"copy (select {q}) to '{d}/meta/{name}.parquet'")

  build_r2.main(d)
  r2 = d / "r2"
  rows = lambda q: con.sql(q).fetchall()

  # product lbs count once per application, not once per AI row: 10 + 4 = 14 in May, not 28
  assert rows(f"select * from '{r2}/summary/county.parquet' order by monthyear") == [
    ("06001", "1", "2022-01", "AG", "A", 5.0, 10.0),
    ("06001", "1", "2023-05", "AG", "G", 7.0, 14.0),
    ("06001", "1", "2023-06", "NON-AG", None, 7.0, 7.0)]
  # use/: AI pounds per row; prd is the whole cell's product lbs on each AI row
  assert rows(f"select monthyear, prodno, chem_code, chm, prd from '{r2}/use/county.parquet' order by all") == [
    ("2022-01", "6", "a", 5.0, 10.0), ("2023-05", "5", "a", 3.0, 14.0), ("2023-05", "5", "b", 4.0, 14.0),
    ("2023-06", "7", "a", 7.0, 7.0)]
  # crosswalks split both pound columns by area; non-ag never reaches the non-county geographies
  assert rows(f"select geo, monthyear, chm, prd from '{r2}/summary/tract.parquet' order by all") == [
    ("t1", "2022-01", 1.25, 2.5), ("t1", "2023-05", 1.75, 3.5), ("t2", "2022-01", 3.75, 7.5), ("t2", "2023-05", 5.25, 10.5)]
  assert rows(f"select geo, monthyear, chem_code, chm, prd from '{r2}/use/tract.parquet' where geo = 't1' order by all") == [
    ("t1", "2022-01", "a", 1.25, 2.5), ("t1", "2023-05", "a", 0.75, 3.5), ("t1", "2023-05", "b", 1.0, 3.5)]
  for geo, key in [("section", sec), ("township", twp), ("school", "s1"), ("zip", "z1")]:
    assert rows(f"select geo, monthyear, chm, prd from '{r2}/summary/{geo}.parquet' order by all") == [
      (key, "2022-01", 5.0, 10.0), (key, "2023-05", 7.0, 14.0)], geo
  # geography table: area in square miles, demography as double (duckdb-wasm turns bigint into JS BigInt)
  geo = con.sql(f"select * from '{r2}/geo/county.parquet'")
  assert geo.fetchall() == [("06001", "Somewhere", 1.0) + (1.0,) * 16]
  assert {str(t) for t in geo.types[2:]} == {"DOUBLE"}, geo.types
  assert json.loads((r2 / "manifest.json").read_text()) == {"format": 1, "start_year": 2022, "end_year": 2023}
  assert json.loads((r2 / "lookup/counties.json").read_text()) == [{"Name": "Alameda County", "CountyCode": "1", "FIPS": "06001"}]

print("build ok")
