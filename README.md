# Pesticide Data Explorer: Californians for Pesticide Reform

Data pipeline for [cpr-explorer-v2](https://github.com/dsi-rse/cpr-explorer-v2). It turns CalPIP pesticide use exports into static parquet files on Cloudflare R2, which the explorer queries in the browser with DuckDB.

## Updating the data

Step-by-step guide with screenshots: [docs/updating-the-data.pdf](docs/updating-the-data.pdf) (regenerate it with `docs/guide/make_guide.py` when the steps change). Setup, the front-end cutover and maintenance: [RUNBOOK.md](RUNBOOK.md).

> **Transition:** until the explorer reads from R2 (see the runbook), Publish data doesn't change the live site, which still reads nectr. The old AWS path, **Actions → Run AWS Batch Job (old pipeline)**, still updates it.

1. Request a year's records from [CalPIP](https://calpip.cdpr.ca.gov) and download the zip.
2. Upload the zip to `calpip/` in the `pesticide-data-raw` R2 bucket with Cyberduck and an uploader key. A zip for a year that's already there replaces it, which is how you re-pull a year.
3. In GitHub, go to **Actions → Build data → Run workflow**. It takes about 5 minutes and doesn't touch the live site. The run's summary page has the new version name, yearly totals next to the live site's, any chemicals missing from PAN's category sheet, and a preview link (`<site>?preview`).
4. If it looks right, run **Actions → Publish data** with that version name. A blank version publishes the latest build.

Each build is an immutable folder (`v<date>-<time>/`) in the `pesticide-data` bucket. Build data points `preview.json` at it; Publish data points `manifest.json` at it. The explorer reads `manifest.json` (or `preview.json` with `?preview`) on load, so publishing needs no FE rebuild. To undo a publish, run Publish data with the previous version, which every publish summary names.

**Two R2 buckets**, so an uploader key can never touch what the site serves:
- `pesticide-data-raw`: inputs, private, a mirror of this repo's `data/` folder. Uploaders write here; CI only reads it.
  - `calpip/`: CalPIP zips, plus `calpip_<year>.parquet` for older years whose zips are gone
  - `pur/meta/`: `chemical.txt`, `product.txt`, `site.txt`, `AI Cat Data.xlsx`, `Restricted Pesticides.xlsx`. Names for codes newer than the `.txt` tables come from the CalPIP exports.
  - `census_data/ca-county-dpr-xwalk.csv`: DPR county codes to FIPS
  - `census_geos/crosswalks/*.parquet`, `output/ca-*-demography.parquet`: static geography, rebuilt only when boundaries or ACS data change (`cli download_geo`, `intersect`, `output`)
- `pesticide-data`: what the site reads, public through a custom domain. Only CI (and admins) write here.
  - `v*/`: builds, immutable. `manifest.json`: the live one. `preview.json`: the latest build. `duckdb-wasm/`: the browser query engine.

**GitHub settings:** a `data` environment limited to `main` holds the secrets `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` (an R2 token with read and write on both buckets). Variables: `R2_BUCKET`, `R2_RAW_BUCKET` and `SITE_URL` (the explorer's URL, used for the preview link). See [RUNBOOK.md](RUNBOOK.md).

**Checks:** `cli/check_clean.py` and `cli/check_build.py` run the clean and build steps on tiny fake datasets in under a second. They run on every pull request and at the start of Build data.

**Run locally:** copy the inputs into `data/` (`rclone copy`, not `sync`: `data/` also holds tracked files), then run the same steps.
```bash
pip install -r requirements.txt
python3 cli/check_clean.py && python3 cli/check_build.py
python3 cli clean && python3 cli meta && python3 cli build_r2
```
Output lands in `data/r2/`.

# Archive
## About

### OSL Data Collaboratory
This project is part of the Open Spatial Lab's 2023 Data Collaboratory. The Collaboratory is a 6-month program where OSL engages with social impact organizations to build a customized tool for data management, analysis, communication, and visualization. Circulate San Diego’s organizational engagement and feedback directly informs this work. 

Based at the University of Chicago Data Science Institute, the Open Spatial Lab creates open source data tools and analytics to solve problems using geospatial data science. Read more about OSL at https://datascience.uchicago.edu/research/open-spatial-lab/. 

### Project Scope
**About**: Californians for Pesticide Reform (CPR) is a statewide coalition of more than 190 organizations that was founded in 1996 to fundamentally shift the way pesticides are used in California.  

**Project**: OSL worked with CPR to develop a new data tool to track and visualize pesticide use across the state of California at multiple spatial scales, including neighborhoods, school districts and counties. This project leverages publicly available pesticide use data to deliver a tracking tool that remains sustainable and stable online, and transparent to update and maintain by CPR and its coalition partners. 

## Data Biography
- PUR data `data/pur/` is collected from 2001 to 2021
- Section data `data/sections` is a 2023 vintage of the PUR sections. `sections.parquet` combining all county data
- `data/geo/cb_2021_06_tract_500k.zip` contains 2021 census tracts for california
- `data/geo/zip-county-crosswalk.xlsx` uses Q3 2023 HUD zip to county crosswalk (https://www.huduser.gov/apps/public/uspscrosswalk/home)
- `data/geo/cb_2020_us_zcta520_500k (1).zip` contains 2020 ZCTA boundaries, which are the most recent see https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2021.html#list-tab-1883739534
- `data/geo/CA-townships-2023.geojson` is 2023 CA townships geometries from CA State Geoportal's [Public Land Survey System (PLSS): Township and Range](https://gis.data.ca.gov/datasets/ea19d0ff6d584755b8153701fa8f4346/explore?location=38.905874%2C-120.194561%2C7.15)
- Community & demographic data `data/community vars/` are from [American Community Survey 2021 (5-Year Estimates)](https://www.socialexplorer.com/data/ACS2021_5yr/metadata/), accessed via Social Explorer. Variables include: A04001 Hispanic of Latino by Race, A12002 Highest Educational Attainment for Population 25 Years and Over, A14006 Median Household Income, and A17004 Industry by Occupation for Employed Population 16 Years and Over. 

## Scripts
- `cli/archive/download_pur.py` helps to download and parse PUR data from 2001 to 2021
- `cli/download_sections.py` helps to download GIS data
