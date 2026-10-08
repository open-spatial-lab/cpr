# Runbook: data pipeline on R2 and GitHub Actions

For developers. The step-by-step guide for people adding data is [docs/updating-the-data.pdf](docs/updating-the-data.pdf).

**Two R2 buckets**, so a leaked uploader key can't change what the site serves:

| Bucket | Holds | Who writes |
|---|---|---|
| `pesticide-data-raw` (private) | inputs, a mirror of `data/` | uploaders (Cyberduck + their own key), admins |
| `pesticide-data` (public via custom domain) | `v*/` builds, `manifest.json`, `preview.json`, `duckdb-wasm/` (code every visitor runs) | CI and admins only |

**GitHub:** the workflows' secrets live in a `data` environment that only runs from `main`, and `main` requires pull requests. Someone with the Write role (needed to start workflows) therefore can't push a branch workflow that reads the secrets.

## State on 2026-10-08

- [x] `pesticide-data` holds `v1/` (it matches the local build; `reference_queries.py` passes 25/25 reading it from R2), `duckdb-wasm/1.33.1-dev57.0/`, and `manifest.json` / `preview.json` / `v1/manifest.json` pointing at `v1`. `v1/` has immutable cache headers.
- [x] A rehearsal of Build data against R2 worked: the workflow's own download filters, then the build. It showed 0.00% for every year against `v1`.
- [ ] `pesticide-data-raw` doesn't exist yet. The inputs are temporarily in `pesticide-data/raw/` and at its root. See step 3.
- [ ] The `data` environment and the protection on `main`. See step 6.
- [ ] CORS, the custom domain, keys, secrets and variables. See steps 1, 2, 5 and 6.

## One-time setup

The commands use rclone with a remote named `r2` (`rclone config`: type `s3`, provider `Cloudflare`, endpoint `https://<account id>.r2.cloudflarestorage.com`, with an admin key that can reach both buckets).

1. **Custom domain, `pesticide-data` only.** In the bucket's Settings, connect a custom domain under Custom Domains. That domain is the FE's `VITE_DATA_URL`. Don't use `r2.dev` for the site: it's rate-limited and not cached. Leave `pesticide-data-raw` with no public access.
2. **CORS on `pesticide-data`.** List every origin that serves or embeds the explorer:
   ```json
   [{"AllowedOrigins": ["https://<explorer site>", "http://localhost:5173"],
     "AllowedMethods": ["GET", "HEAD"], "AllowedHeaders": ["*"],
     "ExposeHeaders": ["Content-Length", "Content-Range", "Accept-Ranges", "ETag"],
     "MaxAgeSeconds": 86400}]
   ```
3. **Split the inputs out.**
   - Create `pesticide-data-raw` in the dashboard. Then do a server-side copy of the inputs, and of the old reference folders worth keeping:
     ```bash
     rclone copy r2:pesticide-data/raw r2:pesticide-data-raw
     rclone copy r2:pesticide-data r2:pesticide-data-raw/reference --include "{community vars,geo,legacy,live,meta,sections,pur}/**" --include "R13494891_SL050.csv"
     ```
   - Check `rclone ls r2:pesticide-data-raw/calpip`, then remove the inputs from the public bucket. List first, delete second:
     ```bash
     rclone lsf r2:pesticide-data --max-depth 1
     rclone purge r2:pesticide-data/raw
     for d in calpip census_data census_geos "community vars" geo legacy live meta output pur sections; do rclone purge "r2:pesticide-data/$d"; done
     rclone deletefile r2:pesticide-data/R13494891_SL050.csv
     ```
   - Afterwards `pesticide-data` should hold only `v1/`, `duckdb-wasm/`, `manifest.json` and `preview.json`.
4. **Pointers and cache headers for `v1`** (done 2026-10-08). The manifest is `data/r2/manifest.json`: `{"format": 1, "start_year": 2017, "end_year": 2023, "version": "v1"}`, written to `v1/manifest.json`, and with `Cache-Control: no-cache` to `manifest.json` and `preview.json`.
5. **Keys**, under R2 → Manage API tokens. Revoke any key that's broader than its holder needs.
   - **CI:** Object Read & Write on both buckets. It goes into the `data` environment (step 6).
   - **Uploaders (PAN):** Object Read & Write on **`pesticide-data-raw` only**, one token per person so each can be revoked alone. Send them three values: Server (`<account id>.r2.cloudflarestorage.com`), Access Key ID and Secret Access Key.
   - **Admins:** use your own keys for rclone. Never hand an uploader a key that reaches `pesticide-data`.
6. **GitHub** (open-spatial-lab/cpr):
   - Under Settings → Environments, create `data` and set Deployment branches to **Selected branches** with the rule `main`. Add the secrets `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` there.
   - Move the old pipeline's `AWS_*` and `BATCH_*` secrets into `data` too, then delete the repo-level copies. Until then, any workflow on any branch can read them.
   - Variables (not secret, repo level is fine): `R2_BUCKET=pesticide-data`, `R2_RAW_BUCKET=pesticide-data-raw`. Set `SITE_URL` only after the FE cutover, so Build summaries don't link to a preview the old site can't show.
   - Under Settings → Branches, protect `main`: require a pull request before merging.
   - Give uploaders the Write role so they can start the workflows, which all run from `main`.
7. **Guide details.** Put the explorer URL into `docs/guide/make_guide.py`, which has a blank for it now, and regenerate (see Routine work).
8. **Smoke test after merge.** The Run workflow button only appears for workflows on `main`. Run **Build data**:
   - `Check the pipeline logic` should pass;
   - the summary should show every year at 0.00% against `v1`, and no chemicals missing categories.

   Don't publish it until the FE has switched over. It differs from `v1` only in township IDs (the fix in PR #2) and in the 24 products and 3 sites now named in the lookups.

## Front-end cutover (cpr-explorer-v2)

1. Implement [fe-migration/PLAN.md](fe-migration/PLAN.md) on a branch. `pnpm parity` must pass against `${VITE_DATA_URL}/v1`.
2. Set `VITE_DATA_URL` (the custom domain) in the FE build environment and deploy.
3. Check the live site and `<site>?preview`, then set the `SITE_URL` variable in this repo.
4. From then on, data updates are Build data, then Publish data, with no FE deploys. Remove the "Until the switch-over" box from the guide and the transition note from the README.

## Retire AWS (after the new FE has run cleanly for a couple of weeks)

Until then, the old path still works: **Actions → Run AWS Batch Job (old pipeline)** starts the frozen ECR image, which updates nectr. Nothing in this repo can rebuild that image, so don't delete it early. If you use it, also copy the zip into `pesticide-data-raw/calpip/` so the two paths stay in step.

1. Export anything worth keeping from the S3 buckets `pesticide-explorer-raw-data`, `test-bucket-osl-cpr` and `cpr-pan-backup`.
2. Delete the Batch job queue, job definition and compute environment, the ECR repository `pan-data-update`, and the S3 buckets.
3. Tear down nectr: the Webiny/Pulumi stack (Lambda, API Gateway, CloudFront, DynamoDB).
4. Delete `.github/workflows/aws-batch.yml`, the `AWS_*` and `BATCH_*` secrets, and the nectr calls in `docs/*.html`.

## Routine work

| Task | How |
|---|---|
| Add a year | Upload the CalPIP zip to `pesticide-data-raw/calpip/`, run Build data, check the summary and preview, then run Publish data with the version name. |
| Re-pull a year | Same, and delete that year's older zip. If both are there, the newest upload wins. |
| Roll back | Run Publish data with the previous version. Every publish summary names the one it replaced. |
| New chemicals in the summary's "no class or health information" list | Add them to `AI Cat Data.xlsx` (PAN), upload it to `pesticide-data-raw/pur/meta/`, run Build data. |
| Update PUR code tables | Optional. Product, site and chemical names missing from the `.txt` tables are taken from the CalPIP exports. Upload newer tables from DPR's annual PUR release to `pesticide-data-raw/pur/meta/` if you want DPR's canonical names. |
| New census geography or ACS data | Locally: `pip install geopandas requests bs4`, then `python3 cli download_geo`, `intersect` and `output`. Upload the changed files to `census_geos/`, `census_data/` and `output/` in `pesticide-data-raw`, then run Build data. |
| Run the pipeline locally | `rclone copy r2:pesticide-data-raw data`. Use **copy, not sync**: `data/` also holds files git tracks (`data/pur/*.json`, `data/sections/sections.parquet`), and `sync` would delete them. To clear stale year files, sync only that folder: `rclone sync r2:pesticide-data-raw/calpip data/calpip`. Then run `python3 cli/check_clean.py && python3 cli/check_build.py && python3 cli clean && python3 cli meta && python3 cli build_r2`. The output is `data/r2/`. `python3 cli compare data/r2 <other build dir>` diffs yearly totals. |
| Check the FE query contract | `python fe-migration/reference_queries.py <dir or URL of a build>` should print "all cases match the API". Against `v1` every case must match. Later builds report `map_township_ag` as an expected difference, as long as no geography went missing. |
| Regenerate the guide | `pip install reportlab && python docs/guide/make_guide.py` |
| Upgrade duckdb, pandas or pyarrow | Bump the pins in `requirements.txt`, run both checks, rebuild, and run the reference check. Every year in `compare` against the live build should stay at 0.00%. |

Old versions are kept on purpose, since they make rollback possible. Each is about 420 MB, which costs well under a cent a month on R2.

## When things break

- **Build data fails at "Download inputs", or downloads nothing**:
  - the `R2_RAW_BUCKET` variable is wrong;
  - the CI token lacks access to the inputs bucket;
  - or the run isn't from `main`, so the `data` environment's secrets aren't available.
- **aws-cli errors mentioning checksums or `x-amz-checksum`**: the workflows set `AWS_REQUEST_CHECKSUM_CALCULATION` and `AWS_RESPONSE_CHECKSUM_VALIDATION` to `when_required`. Keep them if you copy the commands elsewhere.
- **"missing CalPIP columns"**: the uploaded zip is a summary or lacks columns. Re-request it from CalPIP with all output columns and "Summarize the data" unchecked.
- **The FE shows CORS errors**: its origin isn't in the bucket's CORS policy (setup step 2).
- **`git push` times out on large commits**: run `git -c http.postBuffer=524288000 push`.
