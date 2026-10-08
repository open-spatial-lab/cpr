# Reference implementation of every FE data query, as DuckDB SQL over the files from cli/build_r2.py.
# Port these builders to TypeScript; this script checks them against fixtures.json (live nectr API snapshots).
#   python fe-migration/reference_queries.py data/r2                      # local build
#   python fe-migration/reference_queries.py https://<r2-public-host>/v1  # what's on R2 (needs CORS + range support)
import json, sys
from pathlib import Path
import duckdb

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else str(Path(__file__).parent.parent / "data" / "r2")
ATTRS = f"'{BASE}/lookup/chem_attrs.parquet'"

# column name the map tiles join on, per geography
DATA_ID = {"county": "FIPS", "school": "FIPS", "tract": "GEOID", "section": "comtrs", "township": "MeridianTownshipRange", "zip": "Zip Code"}
# demographic range filters (map dual view only): single threshold
DEMOG = {"pctblack": ('"Pct NH Black"', ">="), "pcthispanic": ('"Pct Hispanic"', ">="), "pctwhite": ('"Pct NH White"', ">="),
         "pctasian": ('"Pct NH Asian"', ">="), "pctnativeamerican": ('"Pct NH AIAN"', ">="),
         "pctnativehawaiian": ('"Pct NH NHPI"', ">="), "income": ('"Median HH Income"', "<=")}
# any of these needs the per-AI `use/` table; otherwise the small `summary/` table answers the query
DETAIL_PARAMS = {"site", "category", "health", "ai_type", "chemical", "product"}


def lit(csv):
  """comma-separated filter value -> SQL string list. Values come from the UI, so escape quotes."""
  return ", ".join("'" + v.replace("'", "''") + "'" for v in str(csv).split(","))


def where(p):
  w = [f"monthyear between '{p['start']}' and '{p['end']}'"]
  if p.get("usetype", "*") != "*": w.append(f"usetype = '{p['usetype']}'")
  if p.get("aerial_ground", "*") != "*": w.append(f"aerial_ground in ({lit(p['aerial_ground'])})")
  if "county" in p: w.append(f"county_cd in ({lit(p['county'])})")
  if "site" in p: w.append(f"site_code in ({lit(p['site'])})")
  if "product" in p: w.append(f"prodno in ({lit(p['product'])})")
  if "chemical" in p: w.append(f"chem_code in ({lit(p['chemical'])})")
  if "ai_type" in p: w.append(f"chem_code in (select chem_code from {ATTRS} where ai_type_ID in ({lit(p['ai_type'])}))")
  # class and health ids are pipe-joined per chemical, e.g. 'OP|PYR': match whole ids, not substrings
  for param, col in (("category", "major_category"), ("health", "health")):
    if param in p: w.append(f"chem_code in (select chem_code from {ATTRS} where list_has_any(string_split({col}, '|'), [{lit(p[param])}]))")
  return " and ".join(w)


def use_by(geo, p, keys):
  """chm/prd summed by `keys`. In use/, prd repeats on each AI row of a cell, so max() it per cell before summing."""
  if DETAIL_PARAMS & p.keys():
    return f"""select {keys}, sum(chm) chm, sum(prd) prd from (
        select {keys}, county_cd, monthyear, usetype, aerial_ground, site_code, prodno, sum(chm) chm, max(prd) prd
        from '{BASE}/use/{geo}.parquet' where {where(p)} group by all
      ) group by all"""
  return f"select {keys}, sum(chm) chm, sum(prd) prd from '{BASE}/summary/{geo}.parquet' where {where(p)} group by all"


def map_query(geo, p):
  """one row per geography (left join: geographies without use get null lbs), same columns as the old API"""
  demog = " and ".join(f"{col} {op} {float(p[k])}" for k, (col, op) in DEMOG.items() if k in p) or "true"
  # intensity from the rounded lbs, matching the old API exactly
  return f"""select g.geo "{DATA_ID[geo]}", g.* exclude (geo, sqmi),
      round(u.chm, 2) lbs_chm_used, round(u.prd, 2) lbs_prd_used,
      round(round(u.chm, 2) / g.sqmi, 2) ai_intensity, round(round(u.prd, 2) / g.sqmi, 2) prd_intensity
    from '{BASE}/geo/{geo}.parquet' g left join ({use_by(geo, p, 'geo')}) u using (geo)
    where {demog}"""


def timeseries_query(series, p):
  """statewide monthly series from the county tables; `county` filter narrows it"""
  if series == "product":
    return f"select monthyear, prodno, round(chm, 2) lbs_chm_used, round(prd, 2) lbs_prd_used from ({use_by('county', p, 'monthyear, prodno')})"
  if series == "ai":
    return f"select monthyear, chem_code, round(chm, 2) lbs_chm_used from ({use_by('county', p, 'monthyear, chem_code')})"
  if series == "usetype":
    group, name, keep = "a.ai_type_ID", "ai_type", "true"
  else:  # class: a chemical in several selected classes counts toward each
    group, name, keep = "unnest(string_split(a.major_category, '|'))", "ai_class", f"ai_class in ({lit(p.get('category', ''))})"
  return f"""select monthyear, {name}, round(sum(chm), 2) lbs_chm_used from (
      select monthyear, {group} {name}, chm from ({use_by('county', p, 'monthyear, chem_code')}) u join {ATTRS} a using (chem_code)
    ) where {keep} group by all"""


SERIES_KEY = {"class": "ai_class", "usetype": "ai_type", "ai": "chem_code", "product": "prodno"}


def check():
  fx = json.load(open(Path(__file__).parent / "fixtures.json"))
  con = duckdb.connect()
  con.sql("install httpfs; load httpfs") if BASE.startswith("http") else None
  failures = 0
  for case in fx["cases"]:
    p = case["params"]
    if case["view"] == "map":
      key, cols = DATA_ID[case["geo"]], ["lbs_chm_used", "lbs_prd_used", "ai_intensity", "prd_intensity"]
      got = con.sql(map_query(case["geo"], p)).df()
      problems = [] if len(got) == case["api_row_count"] else [f"{len(got)} rows, api {case['api_row_count']}"]
      got, exp = got[got.lbs_chm_used.notna()].to_dict("records"), case["rows_with_use"]
      key = [key]
    else:
      key, cols = ["monthyear", SERIES_KEY[case["series"]]], ["lbs_chm_used"] + (["lbs_prd_used"] if case["series"] == "product" else [])
      got, exp, problems = con.sql(timeseries_query(case["series"], p)).df().to_dict("records"), case["rows"], []
    g = {tuple(r[k] for k in key): r for r in got}
    if len(g) != len(exp): problems.append(f"{len(g)} rows with use, api {len(exp)}")
    for e in exp:
      r = g.get(tuple(e[k] for k in key))
      if r is None: problems.append(f"missing {[e[k] for k in key]}"); continue
      # use/ stores float32, so filtered totals can differ from the API past the 6th significant digit
      problems += [f"{[e[k] for k in key]} {c}: {r[c]} vs api {e[c]}" for c in cols if abs(r[c] - e[c]) > max(0.02, 1e-4 * abs(e[c]))]
    failures += bool(problems)
    print(f"{'ok  ' if not problems else 'FAIL'} {case['name']:26} {problems[:3]}")
  print("all cases match the API" if not failures else f"{failures} cases differ")
  return failures


if __name__ == "__main__":
  sys.exit(check())
