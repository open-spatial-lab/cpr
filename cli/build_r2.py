# Builds the static files the FE queries in the browser with DuckDB-WASM (replaces nectr).
# Inputs: the outputs of `clean` and `meta`, plus the static geography from `intersect` and `output`. Writes data/r2/.
#   use/{geo}.parquet      one row per geo x month x usetype x method x site x product x AI
#   summary/{geo}.parquet  same without site/product/AI, for queries that don't filter on those
#   geo/{geo}.parquet      one row per geo: name, land area, demography
#   lookup/*.json          filter options, same shape as the old nectr option endpoints
#   lookup/chem_attrs.parquet  chem_code -> class / use type / health ids, for SQL joins
#   manifest.json          year range and file format of the build
import json
from pathlib import Path
import duckdb

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SQ_M_PER_SQ_MI = 2589988.110336
DEMOG_COLS = [
  "Median HH Income", "Pop Total", "Pop NH Black", "Pct NH Black", "Pop Hispanic", "Pct Hispanic",
  "Pop NH White", "Pct NH White", "Pop NH Asian", "Pct NH Asian", "Pop NH AIAN", "Pct NH AIAN",
  "Pop NH NHPI", "Pct NH NHPI", "Pct No High School", "Pct Agriculture",
]


def geos(data_dir):
  """geo: (rows = base rows with a `geo` key and area ratio `r`, demography file, demography key, has "Area Name")"""
  xwalk = data_dir / "census_geos" / "crosswalks"
  county_xwalk = data_dir / "census_data" / "ca-county-dpr-xwalk.csv"
  return {
    "county": (f"select b.*, x.FIPS geo, 1.0 r from base b join read_csv('{county_xwalk}', all_varchar=true) x on b.county_cd = x.DPR_ID::int::varchar",
               "ca-county-demography", "FIPS", True),
    # the FE only allows non-ag use on the county map, so the other geos are ag only
    "section": ("select *, comtrs geo, 1.0 r from base where usetype = 'AG'", "ca-section-demography", "comtrs", False),
    "township": ("select *, township geo, 1.0 r from base where usetype = 'AG' and township is not null",
                 "ca-township-demography", "MeridianTownshipRange", False),
    "tract": (f"select b.*, x.GEOID geo, x.AREA_RATIO r from base b join '{xwalk}/tract_intersections.parquet' x on b.comtrs = x.CO_MTRS where usetype = 'AG'",
              "ca-tract-demography", "GEOID", True),
    "school": (f"select b.*, x.FIPS geo, x.AREA_RATIO r from base b join '{xwalk}/school_district_intersections.parquet' x on b.comtrs = x.CO_MTRS where usetype = 'AG'",
               "ca-school-demography", "FIPS", True),
    "zip": (f"select b.*, x.ZCTA5CE20 geo, x.AREA_RATIO r from base b join '{xwalk}/zip_intersections.parquet' x on b.comtrs = x.CO_MTRS where usetype = 'AG'",
            "ca-zip-demography", "GEOID", False),
  }


CELL = "geo, county_cd, monthyear, usetype, aerial_ground, site_code, prodno"
PARQUET = "(format parquet, compression zstd, row_group_size 122880)"


def main(data_dir=DATA_DIR):
  out = data_dir / "r2"
  for d in ("use", "summary", "geo", "lookup"):
    (out / d).mkdir(parents=True, exist_ok=True)
  con = duckdb.connect()
  # prd_share: a use's product lbs split across its AI rows, so summing any set of whole uses counts each use's product
  # once. A use is the old pipeline's use_id: year, use number, county, COMTRS and product lbs
  con.sql(f"""create table base as select comtrs, county_cd, MeridianTownshipRange township, monthyear, usetype, aerial_ground,
      site_code, prodno, chem_code, lbs_chm_used chm,
      lbs_prd_used / count(*) over (partition by monthyear[1:4], use_number, county_cd, comtrs, lbs_prd_used) prd_share
    from '{data_dir / "calpip" / "calpip_full.parquet"}'""")

  for geo, (rows, demog, key, has_name) in geos(data_dir).items():
    print("building", geo)
    con.sql(f"create or replace view rows as {rows}")
    # prd is the product lbs of the whole cell, repeated on each of its AI rows: take max(prd) per cell when querying.
    # float32 keeps totals within ~1e-6 and cuts ~25% of the bytes a query reads; the summary tables stay exact
    con.sql(f"""copy (select * exclude (prd_part) replace (chm::float as chm), (sum(prd_part) over (partition by {CELL}))::float prd from (
        select {CELL}, chem_code, sum(chm * r) chm, sum(prd_share * r) prd_part from rows group by all
      ) order by monthyear, geo, site_code, prodno, chem_code) to '{out}/use/{geo}.parquet' {PARQUET}""")
    con.sql(f"""copy (select {CELL.replace(', site_code, prodno', '')}, sum(chm * r) chm, sum(prd_share * r) prd
      from rows group by all order by monthyear, geo) to '{out}/summary/{geo}.parquet' {PARQUET}""")
    name = '"Area Name", ' if has_name else ""
    # double, not bigint: duckdb-wasm returns bigint as JS BigInt
    cols = ", ".join(f'"{c}"::double "{c}"' for c in DEMOG_COLS)
    con.sql(f"""copy (select "{key}" geo, {name}ALAND / {SQ_M_PER_SQ_MI} sqmi, {cols}
      from '{data_dir / "output" / demog}.parquet') to '{out}/geo/{geo}.parquet' {PARQUET}""")

  meta = data_dir / "meta"
  county_xwalk = data_dir / "census_data" / "ca-county-dpr-xwalk.csv"
  lookups = {
    "chemicals": f"select chem_code, chem_name, yearly_average from '{meta}/chemicals.parquet' order by chem_name",
    "sites": f"select site_code, site_name, yearly_average from '{meta}/sites.parquet' order by site_name",
    "products": f"select product_code, product_name, yearly_average from '{meta}/products.parquet' order by product_name",
    "chemical_classes": f"select label, id from '{meta}/chemical_class_labels.parquet' order by label",
    "health": f"select label, id from '{meta}/health_labels.parquet' order by label",
    "use_types": f"select ai_type_ID, ai_type from '{meta}/use_type_codes.parquet' order by ai_type",
    "counties": f"""select "Area Name" "Name", DPR_ID::int::varchar "CountyCode", FIPS from read_csv('{county_xwalk}', all_varchar=true) order by 1""",
  }
  for name, q in lookups.items():
    con.sql(f"copy ({q}) to '{out}/lookup/{name}.json' (format json, array true)")
  con.sql(f"""copy (select chem_code, major_category, ai_type ai_type_ID, health from '{meta}/categories.parquet')
    to '{out}/lookup/chem_attrs.parquet' {PARQUET}""")
  # the Build data workflow adds the version; Publish data makes it /manifest.json, which the FE reads at startup
  start, end = con.sql("select min(monthyear)[1:4]::int, max(monthyear)[1:4]::int from base").fetchone()
  # format: bump when the layout or columns change, so the FE can refuse builds it doesn't understand
  (out / "manifest.json").write_text(json.dumps({"format": 1, "start_year": start, "end_year": end}))


if __name__ == "__main__":
  main()
