from pathlib import Path

CURRENT_DIR = Path(__file__).parent
DATA_DIR = CURRENT_DIR.parent / "data"
OUTPUT_DIR = DATA_DIR / "output"

OUTPUT_FILENAMES = [
  {
    "localfilepath": OUTPUT_DIR / "calpip-tract.parquet",
    "s3filepath": "8m4hl3ua8-calpip-tract__dataset__.parquet",
  },
  {
    "localfilepath": OUTPUT_DIR / "calpip-zip.parquet",
    "s3filepath": "8m4hl49sx-calpip-zip__dataset__.parquet",
  },
  {
    "localfilepath": OUTPUT_DIR / "calpip-school.parquet",
    "s3filepath": "8m4hl3gt8-calpip-school__dataset__.parquet",
  },
  {
    "localfilepath": OUTPUT_DIR / "calpip-township.parquet",
    "s3filepath": "8m4hl4s22-calpip-township__dataset__.parquet.parquet",
  },
  {
    "localfilepath": OUTPUT_DIR / "calpip-sections.parquet",
    "s3filepath": "8m4hl31rb-calpip-sections__dataset__.parquet",
  },
  {
    "localfilepath": OUTPUT_DIR / "calpip-county.parquet",
    "s3filepath": "8m4hl545m-calpip-county__dataset__.parquet",
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "use_type_codes.parquet",
    "s3filepath": "9m1l9axjz-use_type_codes__dataset__.parquet"
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "health_labels.parquet",
    "s3filepath": "8m1l8awyy-health_labels__dataset__.parquet"
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "chemical_class_labels.parquet",
    "s3filepath": "8m1l5sn4u-chemical_class_labels__dataset__.parquet"
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "chemical_class_labels.parquet",
    "s3filepath": "8m0y5l1dg-major_category_labels__dataset__.parquet"
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "chemicals.parquet",
    "s3filepath": "8m0ybpzma-chemicals__dataset__.parquet"
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "sites.parquet",
    "s3filepath": "8m0yburja-sites__dataset__.parquet"
  },
  {
    "localfilepath": DATA_DIR / 'meta' / "products.parquet",
    "s3filepath": "8m0ybtxhg-products__dataset__.parquet"
  },
]
  
BUCKETS = {
  "raw": 'pesticide-explorer-raw-data',
  "output": "test-bucket-osl-cpr" #'wby-fm-bucket-e1e0e98'
}