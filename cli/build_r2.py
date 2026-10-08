# Builds the static files the FE queries in the browser with DuckDB-WASM (replaces nectr).
# Inputs are the outputs of the `clean`, `meta` and `output` steps; writes DATA_DIR/r2/.
#   use/{geo}.parquet      one row per geo x month x usetype x method x site x product x AI
#   summary/{geo}.parquet  same without site/product/AI, for queries that don't filter on those
#   geo/{geo}.parquet      one row per geo: name, land area, demography
#   lookup/*.json          filter options, same shape as the old nectr option endpoints
#   lookup/chem_attrs.parquet  chem_code -> class / use type / health ids, for SQL joins
#   manifest.json          year range of the data
import json
import os
from pathlib import Path
import duckdb

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
OUT = DATA_DIR / "r2"
SQ_M_PER_SQ_MI = 2589988.110336
DEMOG_COLS = [
  "Median HH Income", "Pop Total", "Pop NH Black", "Pct NH Black", "Pop Hispanic", "Pct Hispanic",
  "Pop NH White", "Pct NH White", "Pop NH Asian", "Pct NH Asian", "Pop NH AIAN", "Pct NH AIAN",
  "Pop NH NHPI", "Pct NH NHPI", "Pct No High School", "Pct Agriculture",
]
XWALK = DATA_DIR / "census_geos" / "crosswalks"
COUNTY_XWALK = DATA_DIR / "census_data" / "ca-county-dpr-xwalk.csv"

# geo: (rows = base rows with a `geo` key and area ratio `r`, demography file, demography key, has "Area Name")
GEOS = {
  "county": (f"select b.*, x.FIPS geo, 1.0 r from base b join read_csv('{COUNTY_XWALK}', all_varchar=true) x on b.county_cd = x.DPR_ID::int::varchar",
             "ca-county-demography", "FIPS", True),
  # the FE only allows non-ag use on the county map, so the other geos are ag only
  "section": ("select *, comtrs geo, 1.0 r from base where usetype = 'AG'", "ca-section-demography", "comtrs", False),
  "township": ("select *, township geo, 1.0 r from base where usetype = 'AG' and township is not null",
               "ca-township-demography", "MeridianTownshipRange", False),
  "tract": (f"select b.*, x.GEOID geo, x.AREA_RATIO r from base b join '{XWALK}/tract_intersections.parquet' x on b.comtrs = x.CO_MTRS where usetype = 'AG'",
            "ca-tract-demography", "GEOID", True),
  "school": (f"select b.*, x.FIPS geo, x.AREA_RATIO r from base b join '{XWALK}/school_district_intersections.parquet' x on b.comtrs = x.CO_MTRS where usetype = 'AG'",
             "ca-school-demography", "FIPS", True),
  "zip": (f"select b.*, x.ZCTA5CE20 geo, x.AREA_RATIO r from base b join '{XWALK}/zip_intersections.parquet' x on b.comtrs = x.CO_MTRS where usetype = 'AG'",
          "ca-zip-demography", "GEOID", False),
}
CELL = "geo, county_cd, monthyear, usetype, aerial_ground, site_code, prodno"
PARQUET = "(format parquet, compression zstd, row_group_size 122880)"


def main():
  for d in ("use", "summary", "geo", "lookup"):
    (OUT / d).mkdir(parents=True, exist_ok=True)
  con = duckdb.connect()
  # prd_share: a use's product lbs split across its AI rows, so summing any set of whole uses counts each use's product once
  con.sql(f"""create table base as select comtrs, county_cd, MeridianTownshipRange township, monthyear, usetype, aerial_ground,
      site_code, prodno, chem_code, lbs_chm_used chm,
      lbs_prd_used / count(*) over (partition by monthyear[1:4], use_number) prd_share
    from '{DATA_DIR / "calpip" / "calpip_full.parquet"}'""")

  for geo, (rows, demog, key, has_name) in GEOS.items():
    print("building", geo)
    con.sql(f"create or replace view rows as {rows}")
    # prd is the product lbs of the whole cell, repeated on each of its AI rows: take max(prd) per cell when querying.
    # float32 keeps totals within ~1e-6 and cuts ~25% of the bytes a query reads; the summary tables stay exact
    con.sql(f"""copy (select * exclude (prd_part) replace (chm::float as chm), (sum(prd_part) over (partition by {CELL}))::float prd from (
        select {CELL}, chem_code, sum(chm * r) chm, sum(prd_share * r) prd_part from rows group by all
      ) order by monthyear, geo, site_code, prodno, chem_code) to '{OUT}/use/{geo}.parquet' {PARQUET}""")
    con.sql(f"""copy (select {CELL.replace(', site_code, prodno', '')}, sum(chm * r) chm, sum(prd_share * r) prd
      from rows group by all order by monthyear, geo) to '{OUT}/summary/{geo}.parquet' {PARQUET}""")
    name = '"Area Name", ' if has_name else ""
    # double, not bigint: duckdb-wasm returns bigint as JS BigInt
    cols = ", ".join(f'"{c}"::double "{c}"' for c in DEMOG_COLS)
    con.sql(f"""copy (select "{key}" geo, {name}ALAND / {SQ_M_PER_SQ_MI} sqmi, {cols}
      from '{DATA_DIR / "output" / demog}.parquet') to '{OUT}/geo/{geo}.parquet' {PARQUET}""")

  meta = DATA_DIR / "meta"
  lookups = {
    "chemicals": f"select chem_code, chem_name, yearly_average from '{meta}/chemicals.parquet' order by chem_name",
    "sites": f"select site_code, site_name, yearly_average from '{meta}/sites.parquet' order by site_name",
    "products": f"select product_code, product_name, yearly_average from '{meta}/products.parquet' order by product_name",
    "chemical_classes": f"select label, id from '{meta}/chemical_class_labels.parquet' order by label",
    "health": f"select label, id from '{meta}/health_labels.parquet' order by label",
    "use_types": f"select ai_type_ID, ai_type from '{meta}/use_type_codes.parquet' order by ai_type",
    "counties": f"""select "Area Name" "Name", DPR_ID::int::varchar "CountyCode", FIPS from read_csv('{COUNTY_XWALK}', all_varchar=true) order by 1""",
  }
  for name, q in lookups.items():
    con.sql(f"copy ({q}) to '{OUT}/lookup/{name}.json' (format json, array true)")
  con.sql(f"""copy (select chem_code, major_category, ai_type ai_type_ID, health from '{meta}/categories.parquet')
    to '{OUT}/lookup/chem_attrs.parquet' {PARQUET}""")
  # the Build data workflow adds the version; Publish data makes it /manifest.json, which the FE reads at startup
  start, end = con.sql("select min(monthyear)[1:4]::int, max(monthyear)[1:4]::int from base").fetchone()
  (OUT / "manifest.json").write_text(json.dumps({"start_year": start, "end_year": end}))


if __name__ == "__main__":
  main()
