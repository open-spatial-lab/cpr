# Markdown table of yearly totals in a new build vs the live one, for the "Build data" job's summary page.
#   python3 cli compare <new build dir> [<live build dir>]
import sys
from pathlib import Path
import duckdb


def totals(build_dir):
  return f"select monthyear[1:4] yr, sum(chm) chm, sum(prd) prd from '{build_dir}/summary/county.parquet' group by 1"


def change(new, old):
  if old is None:
    return "new year"
  if new is None:
    return "**removed**"
  if not old:
    return "n/a"
  pct = round(100 * (new - old) / old, 2)
  return "0.00%" if pct == 0 else f"{pct:+.2f}%"  # 0.00%, not -0.00% from float noise


def main():
  new_dir, live_dir = sys.argv[2], (sys.argv[3] if len(sys.argv) > 3 else None)
  live = totals(live_dir) if live_dir else "select null yr, null::double chm, null::double prd where false"
  rows = duckdb.sql(f"""select yr, n.chm, n.prd, l.chm, l.prd from ({totals(new_dir)}) n
    full join ({live}) l using (yr) order by yr""").fetchall()
  print("| Year | Lbs active ingredient | Lbs product | Change vs live site (AI / product) |")
  print("|---|---:|---:|---|")
  for yr, chm, prd, live_chm, live_prd in rows:
    fmt = lambda v: f"{v:,.0f}" if v is not None else "-"
    print(f"| {yr} | {fmt(chm)} | {fmt(prd)} | {change(chm, live_chm) if live_dir else '-'} / {change(prd, live_prd) if live_dir else '-'} |")
  print("\nYears that were already published should show 0.00% unless that year was re-pulled from CalPIP.")
  # PAN's category sheet can't be derived from CalPIP, so flag chemicals in the data that it doesn't list.
  # Compare with the sheet itself: chem_attrs leaves out adjuvants on purpose, as nectr did.
  sheet = Path(__file__).resolve().parent.parent / "data" / "pur" / "meta" / "AI Cat Data.xlsx"
  if not sheet.exists():
    return
  import pandas as pd
  listed = pd.read_excel(sheet, usecols=["CHEM_CODE"])["CHEM_CODE"].astype(str).tolist()
  missing = duckdb.sql(f"""select chem_code::varchar, chem_name from read_json('{new_dir}/lookup/chemicals.json')
    where chem_code::varchar not in (select unnest(?::varchar[])) order by chem_name""", params=[listed]).fetchall()
  if missing:
    print(f"\n**{len(missing)} chemicals have no class, use type or health information**, so those filters skip them. "
          "Add them to `pur/meta/AI Cat Data.xlsx` in the inputs bucket: " + ", ".join(f"{name} ({code})" for code, name in missing))


if __name__ == "__main__":
  main()
