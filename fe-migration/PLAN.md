# Plan: replace the nectr API with in-browser DuckDB queries (cpr-explorer-v2)

**For:** an agent working in `dsi-rse/cpr-explorer-v2`.
**Goal:** the FE stops calling nectr (`VITE_DATA_ENDPOINT`, a Lambda + DynamoDB + Webiny stack) and instead runs DuckDB-WASM in the browser. It reads static parquet files from Cloudflare R2 with HTTP range requests, so each query downloads only the columns and row groups it needs. Rows reaching `staticData` and the options UI must keep the shape they have today, so components don't change.

Everything in the [`fe-migration/`](https://github.com/open-spatial-lab/cpr/tree/main/fe-migration) folder of `open-spatial-lab/cpr` is input to this plan:

- `reference_queries.py` builds every query the FE makes, as DuckDB SQL. **Port it to TypeScript. It is the spec.**
- `fixtures.json` holds 25 snapshots of the live nectr API (2026-10-08), covering every view and filter type. The reference SQL reproduces all 25 against `v1` (`python fe-migration/reference_queries.py <base>/v1` → "all cases match the API").
- `../cli/build_r2.py` is the pipeline step that produces the data files. You don't need to run it: `v1` is on R2 in the `pesticide-data` bucket, and the `manifest.json` / `preview.json` pointers are added during setup (`RUNBOOK.md` in the cpr repo).

The FE repo had in-progress lint/prettier changes on `main` when this was written. Branch from the latest `origin/main`.

---

## 1. Data on R2 (read-only for you)

`VITE_DATA_URL` is the bucket root, for example `https://<r2-custom-domain>`. Ask Dylan for the value.

**Versions and the manifest.** Each data build is an immutable folder (`/v1`, `/v20270115-0930`, …), cached forever. `GET ${VITE_DATA_URL}/manifest.json` (served `no-cache`) says which one is live:

```json
{"format": 1, "version": "v1", "start_year": 2017, "end_year": 2023}
```

`format` is the file layout described below. If it isn't `1`, show the existing error state rather than querying: an older or newer build may have different files or columns.

At startup, fetch the manifest and read every data file from `${VITE_DATA_URL}/${version}/…`. A pipeline run in the `cpr` repo publishes a new version by rewriting the manifest, so **data updates need no FE rebuild or deploy**. The paths in the table below are relative to the version folder.

**Preview link.** The pipeline's "Build data" job points `${VITE_DATA_URL}/preview.json` (same shape) at each new build before anyone publishes it. When the page URL has `?preview`, read `preview.json` instead of `manifest.json`, and show a small fixed banner: "Previewing data version X (years A–B)". Don't call it unpublished: `preview.json` keeps pointing at the latest build after it's published. That's the whole test page; reviewers check a build on the real site before running "Publish data".

| Path | Rows | Columns |
|---|---|---|
| `use/{geo}.parquet` | one per geo × month × usetype × method × site × product × AI | `geo, county_cd, monthyear, usetype, aerial_ground, site_code, prodno, chem_code, chm (float32), prd (float32)` |
| `summary/{geo}.parquet` | one per geo × county × month × usetype × method | `geo, county_cd, monthyear, usetype, aerial_ground, chm, prd` (double) |
| `geo/{geo}.parquet` | one per geography | `geo, ["Area Name"], sqmi, <16 demography columns>` (all numbers double) |
| `lookup/{name}.json` | filter options | same JSON the nectr option endpoints returned |
| `lookup/chem_attrs.parquet` | one per chemical | `chem_code, major_category ('OP\|PYR'), ai_type_ID, health ('CARC\|RM')` |

`{geo}` ∈ `county, section, township, tract, school, zip`. `monthyear` is `'YYYY-MM'`. `county_cd` is the DPR county code with no leading zero (`'1'`–`'58'`), the same as the `CountyCode` option value. Files are sorted by `monthyear`, so a one-year query skips other years' row groups.

**What `prd` means in `use/`:** it is the product pounds of the whole cell (geo, county_cd, monthyear, usetype, aerial_ground, site_code, prodno), repeated on each AI row of that cell. Always take `max(prd)` per cell, then sum (see `use_by()`). Summing `prd` directly double-counts multi-AI products.

**Old ID → new name:**

| Kind | nectr id → new name |
|---|---|
| Map layers (`src/config/map.tsx` `endpoint`) | `674513830507860008cda249` county · `6744f63adb91810008024959` township · `674518c90507860008cda24f` school · `6744f2bbdb91810008024956` tract · `67451c010507860008cda252` section · `674516220507860008cda24c` zip |
| Timeseries (`timeseriesViews[].endpoint`) | `6745e03480f7590008e360de` class · `6745e0fa80f7590008e360df` usetype · `6745e18280f7590008e360e0` ai · `6745e1d780f7590008e360e1` product |
| Options (`options.endpoint`) | `66d8a22692868e000864e898` sites · `66e1e112c640880008ba68f2` chemical_classes · `66d88452ae7ce10008a9473f` chemicals · `66d8a20a92868e000864e897` products · `66d88483ae7ce10008a94741` use_types · `66d8a25592868e000864e899` health · `66db44c7f96f070008c08e39` counties |

## 2. Implementation steps

1. **Dependencies.** Add `@duckdb/duckdb-wasm`. Remove `msgpackr` (only `store.ts` uses it) and delete `src/utils/constructQuery.ts` once nothing imports it. The repo was mid-cleanup when this was written (see its `CODE_REVIEW.md`), so work from the current code, not line numbers quoted here.
2. **`src/utils/db.ts` (new).**
   - Lazy singleton `AsyncDuckDB`; `query(sql): Promise<Record<string, unknown>[]>` converting Arrow rows with `.toJSON()`.
   - Start initialization on app load, in idle time, so the first query doesn't pay for the WASM download.
   - Bundles: Cloudflare Workers static assets cap single files at 25 MiB, so don't ship the `.wasm` in `dist/`. Dylan has self-hosted a bundle on R2 at `${VITE_DATA_URL}/duckdb-wasm/1.33.1-dev57.0/` (the `duckdb-browser-eh` worker and wasm). Use it with `@duckdb/duckdb-wasm` at the matching version. The parquet extension is there too (`extensions/v1.5.4/wasm_eh/`), so run `SET custom_extension_repository = '${VITE_DATA_URL}/duckdb-wasm/1.33.1-dev57.0/extensions'` after init; the app then never fetches code from a third-party CDN. The data files were written by DuckDB 1.5.6 and the browser build is 1.5.4, the same release line. The parity test (step 9) confirms the browser build reads them correctly.
3. **`src/utils/queries.ts` (new).** Port `where`, `use_by`, `map_query`, and `timeseries_query` from `reference_queries.py`, unchanged in logic.
   - Inputs are a params object `Record<string, string>` built exactly as `constructQuery` builds URL params today:
     - only filters whose label is in `filterKeys`;
     - empty values skipped;
     - arrays joined with `,`;
     - the date range expands to `start`/`end`;
     - `'*'` means no filter.
   - Easiest: turn `constructQuery` into `buildParams` that returns that object.
   - Escape `'` in every literal (`lit()`). Values can arrive from saved or shared selections, so escaping alone isn't enough. The Python reference is safe because of three more things, and the port needs them too:
     - numbers (the demographic thresholds) must pass `Number.isFinite`;
     - `geo` and `series` must be keys of their lookup tables;
     - `usetype` must be `AG`, `NON-AG` or `*`, and `start`/`end` must match `YYYY-MM`.

     Never build SQL from a value that failed one of these.
4. **Config.**
   - `map.tsx`: `endpoint` → `geo: 'county' | …`.
   - `filters.tsx`: timeseries `endpoint` → `series`; option `endpoint` → lookup name (mapping above).
   - Keep the `dataId` / `tileId` values. The SQL aliases `geo` to the old key column (`FIPS`, `GEOID`, `comtrs`, `MeridianTownshipRange`, `Zip Code`).
5. **`src/state/store.ts` `executeQuery`.**
   - Replace `constructQuery` + `fetch` + `unpack` + the JSON retry with `query(mapQuery(geo, params))` or `query(timeseriesQuery(series, params))`.
   - Leave the rest alone: loading states, the `ag-on-not-counties` guard, the timeseries 1–10 value checks, `infillTimeseries`, `timestamp`, and the `try/catch` that sets `loadingState: "error"`. DuckDB init or query failures must land there too.
   - Also discard results from superseded queries: if the user changes filters mid-query, an older, slower query must not overwrite newer `staticData`.
6. **`src/hooks/useOptions.ts`.** Fetch `${dataBase}/lookup/${name}.json`, where `dataBase` = `${VITE_DATA_URL}/${manifest.version}`. Plain fetch, no DuckDB.
7. **Years from the manifest.** `START_YEAR` / `END_YEAR` in `src/dates.ts` are build-time constants, rewritten by `scripts/update-years.py` from a nectr endpoint. Make them come from the manifest instead:
   - `dates.ts` exports `let START_YEAR, END_YEAR` and a `setYears()`.
   - `main.tsx` fetches the manifest, calls `setYears()`, then dynamically imports `App`. The config modules (`filters.tsx`, `map.tsx`) read the years when they're first evaluated, so they must load after this.
   - Keep the manifest's `version` alongside (same module or a small `dataBase` export) for steps 2, 5 and 6.
   - If the manifest fetch fails, show the existing error state, not a blank page.
   - Delete `scripts/update-years.py` and the "Update years" step in `.github/workflows/test.yml`.
8. **Env and docs.** Rename `VITE_DATA_ENDPOINT` to `VITE_DATA_URL` in `.env.example`, the README (drop the "NECTR endpoint" wording), and `.github/workflows/build.yml` (a new `VITE_DATA_URL` secret or var).
9. **Parity test (new, required).** `scripts/parity.ts`, run with `pnpm parity` via `tsx`, using `@duckdb/node-api` as a devDependency.
   - Import the TS builders from `src/utils/queries.ts` and run every case in `fixtures.json` (copy it into the repo) against **`${VITE_DATA_URL}/v1`, pinned, not the manifest's version**. The fixtures are snapshots of the old API, and `v1` was built from the same data. Later builds legitimately differ: for example, the new pipeline places 182,696 lbs on townships that the old one dropped, so `map_township_ag` changes by design. Don't port `EXPECTED_AFTER_V1`: against `v1` every case must match.
   - Compare the same way `reference_queries.py check()` does:
     - map row count equals `api_row_count`;
     - rows with use match on key;
     - tolerance `max(0.02, 1e-4 × |value|)`.
   - Add it to CI.
10. **CalPIP match tests** (`scripts/tests/`). They call the nectr URL in `config.py`. Switch `get_test_data` to run the reference SQL (copy `reference_queries.py` into `scripts/tests/`) with Python `duckdb` + `httpfs` against the manifest's current version (these compare to CalPIP, not to the old API). Then re-run them and refresh the README table, which is stale (see §4).

## 3. Behavior to keep (the fixtures cover all of these)

- **Map results are a left join over every geography:** 58 counties, 9,109 tracts, 867 school districts, 1,800 zips, 4,772 townships, 163,799 sections. Geographies without use get `null` lbs. `cleanRowsForView` already drops those for display. Keep the full set for parity first; returning only rows with use is an optional later optimization if the map styling doesn't need the rest.
- **Demographic filters (dual view only) remove geographies.** `pct*` are minimum thresholds (`>=`), `income` is a maximum (`<=`). Each is a single number from the range slider.
- **Non-county geographies only have ag data.** The existing `ag-on-not-counties` guard already stops non-ag queries for them; keep it.
- **Product pounds under any AI-level filter** (chemical, class, health, use type) are the full product pounds of applications that contain a matching AI. They are not a per-AI share. `use_by()` gets this from `max(prd)` per cell.
- **Class and health filters match whole IDs from pipe-joined lists** (`list_has_any(string_split(…, '|'), […])`), not substrings.
- **Intensity** = `round(round(lbs, 2) / sqmi, 2)`. Computing it from rounded pounds matches the old API exactly.
- **Timeseries keys:**
  - class series: single class IDs, and a chemical in two selected classes counts toward both;
  - use-type series: `ai_type` as a string ID (`'0'`, `'5'`);
  - product series: returns both `lbs_chm_used` and `lbs_prd_used`.
  - Empty results (`[]`) are legitimate. Two fixtures are empty, so show the existing "no data" state.
- **Summary vs detail:** queries with no site, product, or AI-level filter read `summary/`, which is exact and small. Anything else reads `use/`. That choice lives in `use_by()`; don't bypass it.

**Intentional differences from the old API** (fine; the fixtures don't check them):

- Every geography now has all 16 demography columns. nectr's hand-built column lists dropped a few: tracts lacked `Pop NH NHPI` and `Pct No High School`, zips lacked `Pop NH White`, townships lacked `Pop NH AIAN`. Check that downloads and the table view handle the extra columns.
- Filtered (`use/`) totals are float32-based, so they can differ from the old API in the 7th significant digit.

## 4. Context you'll see in tests (not FE bugs, don't "fix")

- **CalPIP ground truth undercounts some multi-AI products.** CalPIP's query tool collapses AI rows within one application that have identical pounds and percent, for example propiconazole and tebuconazole in WOLMAN E. That's why Mendocino product pounds are +21% vs CalPIP. Our numbers are the correct ones.
- **The 2022 ground-truth files are from Dec 2024; the data is from Mar 2026.** Expect about 0.4% diffuse drift for 2022 until someone re-pulls those files.
- **The README test table (−10.28% for 2022 product) is stale.** Against today's data it's +0.81%.
- **ZCTA totals run about 1.9% low by construction.** Some sections fall in no ZCTA.

## 5. Performance targets

| Query | Reads (one year) | Old API |
|---|---|---|
| County, no site/product/AI filter | ~0.3 MB | 28 KB, 1–2 s |
| County, filtered | ≤ 6.4 MB | 28 KB, 1–2 s |
| Section, no filter / filtered | ~3 MB / ≤ 15 MB | 66 MB JSON, 6.5 s |
| Tract, school, zip, township, filtered | 7.5–10.4 MB | 0.4–4 MB |

**Measure in the browser** (DevTools network tab, cold and warm): DuckDB init time, the default county map, one filtered section query, and one timeseries.
- Goal: default views under ~1 s after init, and filtered one-year section queries in a few seconds on broadband.
- If filtered county queries feel slow, report back rather than redesigning. The pipeline can add a per-site table cheaply.

## 6. Done when

- [ ] No references to `VITE_DATA_ENDPOINT`, nectr IDs, `constructQuery`, or `msgpackr` remain in `src/`.
- [ ] `pnpm parity`: all 25 fixture cases pass against `${VITE_DATA_URL}/v1`.
- [ ] Pointing `manifest.json` at another version switches the site's data and year range on reload, with no rebuild.
- [ ] `?preview` loads the version in `preview.json` and shows the preview banner; without it, the site never reads `preview.json`.
- [ ] Every map layer × view (map, dual view), every timeseries type, all filter dropdowns, the data table, CSV/XLSX downloads, and saved/shared selections work in `pnpm dev` against R2.
- [ ] `pnpm build` and lint pass; the WASM bundle loads in the production build (Workers asset size checked).
- [ ] Performance numbers from §5 reported in the PR description.
- [ ] nectr is not shut down by this PR. That happens after the new FE is live.

**Out of scope:** pipeline changes in the `cpr` repo, map tilesets, and UI redesign.
