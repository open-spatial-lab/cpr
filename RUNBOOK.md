# Runbook: data pipeline on R2 and GitHub Actions

For developers. The step-by-step guide for people adding data is [docs/updating-the-data.pdf](docs/updating-the-data.pdf).

Bucket: `pesticide-data` (Cloudflare R2). People and CI use **S3 API keys** scoped to this bucket, not Cloudflare account roles. Uploaders use Cyberduck with a key; GitHub Actions uses another.

## State on 2026-10-08

- [x] Bucket `pesticide-data` exists.
- [x] `v1/` is uploaded and matches the local build (schema, rows and sums of all 26 files). `reference_queries.py` passes 25/25 against it.
- [x] The DuckDB-WASM bundle is at `duckdb-wasm/1.33.1-dev57.0/`.
- [ ] Inputs are at the bucket root (`calpip/`, `pur/meta/`, …), but Build data reads `raw/`. See setup step 3.
- [ ] `manifest.json`, `preview.json` and `v1/manifest.json` are missing. See setup step 4.
- [ ] `v1/` objects have no `Cache-Control` header. See setup step 4.
- [ ] CORS can't be checked with an object-scoped key. See setup step 2.
- [ ] GitHub secrets and variables aren't set. See setup step 5.

## One-time setup

The commands use rclone with a remote named `r2` (`rclone config`: type `s3`, provider `Cloudflare`, endpoint `https://<account id>.r2.cloudflarestorage.com`). Cyberduck works for any single step too.

1. **Custom domain.** In the Cloudflare dashboard, open the bucket's Settings and connect a custom domain under Custom Domains. That domain is the FE's `VITE_DATA_URL`. Don't use `r2.dev` for the site: it's rate-limited and not cached.
2. **CORS.** In the bucket's Settings, under CORS policy, add every origin that serves or embeds the explorer:
   ```json
   [{"AllowedOrigins": ["https://<explorer site>", "http://localhost:5173"],
     "AllowedMethods": ["GET", "HEAD"], "AllowedHeaders": ["*"],
     "ExposeHeaders": ["Content-Length", "Content-Range", "Accept-Ranges", "ETag"],
     "MaxAgeSeconds": 86400}]
   ```
3. **Move inputs under `raw/`.** These are server-side copies, so nothing is downloaded. Copy only what the pipeline reads:
   ```bash
   rclone copy r2:pesticide-data r2:pesticide-data/raw --include "calpip/calpip_20*.parquet" --include "pur/meta/**" --include "census_data/ca-county-dpr-xwalk.csv" --include "census_geos/crosswalks/**" --include "output/ca-*-demography.parquet"
   ```
   The other root folders (`geo/`, `sections/`, `legacy/`, `live/`, `community vars/`, …) are reference copies of the old S3 bucket. Once the first build works, move them under `raw/` or delete them. Keep `duckdb-wasm/`.
4. **Pointers and cache headers for `v1`.** The manifest is `data/r2/manifest.json`: `{"format": 1, "start_year": 2017, "end_year": 2023, "version": "v1"}`.
   ```bash
   rclone copyto data/r2/manifest.json r2:pesticide-data/v1/manifest.json
   rclone copyto data/r2/manifest.json r2:pesticide-data/manifest.json --header-upload "Cache-Control: no-cache"
   rclone copyto data/r2/manifest.json r2:pesticide-data/preview.json --header-upload "Cache-Control: no-cache"
   rclone copy data/r2 r2:pesticide-data/v1 --exclude manifest.json --header-upload "Cache-Control: public, max-age=31536000, immutable"
   ```
   The last command re-uploads `v1` with the immutable cache header (about 420 MB).
5. **Keys.** Create these under R2, Manage API tokens:
   - **CI:** Object Read & Write on `pesticide-data`. In GitHub (open-spatial-lab/cpr, Settings, Secrets and variables, Actions), set the secrets `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`, and the variable `R2_BUCKET=pesticide-data`. Set `SITE_URL` only after the FE cutover, so Build summaries don't link to a preview the old site can't show.
   - **Uploaders (PAN):** a separate Object Read & Write token on `pesticide-data`, one per person, so one can be revoked alone. Send them three values: Server (`<account id>.r2.cloudflarestorage.com`), Access Key ID, and Secret Access Key. R2 tokens can't be limited to a folder, so the guide tells uploaders to stay in `raw/calpip/`. If that's not enough, move the inputs to a second bucket and give uploaders a key for that one only.
   - Give uploaders the Write role on open-spatial-lab/cpr so they can run Actions.
6. **Guide details.** Put the explorer URL into `docs/guide/make_guide.py`, which has a blank for it now, and regenerate (see Routine work).
7. **Smoke test after merge.** The Run workflow button only appears for workflows on `main`. Run **Build data**:
   - `Check the cleaning logic` should pass;
   - the summary should show every year at 0.00% against `v1`, and no chemicals missing categories.

   Don't publish it until the FE has switched over. It differs from `v1` only in township IDs, which is the fix described in PR #2.

## Front-end cutover (cpr-explorer-v2)

1. Implement [fe-migration/PLAN.md](fe-migration/PLAN.md) on a branch. `pnpm parity` must pass against `${VITE_DATA_URL}/v1`.
2. Set `VITE_DATA_URL` (the custom domain) in the FE build environment and deploy.
3. Check the live site and `<site>?preview`, then set the `SITE_URL` variable in this repo.
4. From then on, data updates are Build data, then Publish data, with no FE deploys. Remove the "Until the switch-over" box from the guide and the transition note from the README.

## Retire AWS (after the new FE has run cleanly for a couple of weeks)

Until then, the old path still works: **Actions → Run AWS Batch Job (old pipeline)** starts the frozen ECR image, which updates nectr. Nothing in this repo can rebuild that image, so don't delete it early. If you use it, also copy the zip into R2 `raw/calpip/` so the two paths stay in step.

1. Export anything worth keeping from the S3 buckets `pesticide-explorer-raw-data`, `test-bucket-osl-cpr` and `cpr-pan-backup`.
2. Delete the Batch job queue, job definition and compute environment, the ECR repository `pan-data-update`, and the S3 buckets.
3. Tear down nectr: the Webiny/Pulumi stack (Lambda, API Gateway, CloudFront, DynamoDB).
4. Delete `.github/workflows/aws-batch.yml`, the `AWS_*` and `BATCH_*` secrets, and the nectr calls in `docs/*.html`.

## Routine work

| Task | How |
|---|---|
| Add a year | Upload the CalPIP zip to `raw/calpip/`, run Build data, check the summary and preview, then run Publish data with the version name. |
| Re-pull a year | Same, and delete that year's older zip. If both are there, the newest upload wins. |
| Roll back | Run Publish data with the previous version. Every publish summary names the one it replaced. |
| New chemicals in the summary's "no class or health information" list | Add them to `AI Cat Data.xlsx` (PAN), upload it to `raw/pur/meta/`, run Build data. |
| Update PUR code tables | Optional. Product, site and chemical names missing from the `.txt` tables are taken from the CalPIP exports. Upload newer tables from DPR's annual PUR release to `raw/pur/meta/` if you want DPR's canonical names. |
| New census geography or ACS data | Locally: `pip install geopandas requests bs4`, then `python3 cli download_geo`, `intersect` and `output`. Upload the changed files under `raw/census_geos/`, `raw/census_data/` and `raw/output/`, then run Build data. |
| Run the pipeline locally | `rclone sync r2:pesticide-data/raw data`, then `python3 cli/check_clean.py && python3 cli clean && python3 cli meta && python3 cli build_r2`. The output is `data/r2/`. `python3 cli compare data/r2 <other build dir>` diffs yearly totals. |
| Check the FE query contract | `python fe-migration/reference_queries.py <dir or URL of v1>` should print "all cases match the API". Later builds report `map_township_ag` as an expected difference. |
| Regenerate the guide | `pip install reportlab && python docs/guide/make_guide.py` |
| Upgrade duckdb, pandas or pyarrow | Bump the pins in `requirements.txt`, rebuild, and run the reference check. Every year in `compare` against the live build should stay at 0.00%. |

Old versions are kept on purpose, since they make rollback possible. Each is about 420 MB, which costs well under a cent a month on R2.

## When things break

- **Build data fails at "Download inputs", or downloads nothing**: the inputs aren't under `raw/` (setup step 3), the secret or variable names are wrong, or the token lacks access to the bucket.
- **aws-cli errors mentioning checksums or `x-amz-checksum`**: the workflows set `AWS_REQUEST_CHECKSUM_CALCULATION` and `AWS_RESPONSE_CHECKSUM_VALIDATION` to `when_required`. Keep them if you copy the commands elsewhere.
- **"missing CalPIP columns"**: the uploaded zip is a summary or lacks columns. Re-request it from CalPIP with all output columns and "Summarize the data" unchecked.
- **The FE shows CORS errors**: its origin isn't in the bucket's CORS policy (setup step 2).
- **`git push` times out on large commits**: run `git -c http.postBuffer=524288000 push`.
