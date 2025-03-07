import boto3
import os
from constants import BUCKETS, OUTPUT_FILENAMES
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')
 
CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent
DATA_DIR = BASE_DIR / 'data'

class DataDownloader:
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3 = boto3.client('s3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_REGION_NAME'))

    def download_file(
        self,
        s3filepath,
        outputfilepath
    ):
        Path(outputfilepath).parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket, s3filepath, outputfilepath)
    
    def download_folder(
            self,
            s3folderpath,
            outputfolderpath
    ):
        Path(outputfolderpath).mkdir(parents=True, exist_ok=True)
        for obj in self.s3.list_objects_v2(Bucket=self.bucket, Prefix=s3folderpath)['Contents']:
            Path( outputfolderpath / obj['Key']).parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.bucket, obj['Key'], outputfolderpath / obj['Key'])

    def upload_file(
        self,
        s3filepath,
        localfilepath
    ):
        self.s3.upload_file(localfilepath, self.bucket, s3filepath)

    def move_file(
            self,
            bucket1,
            s3filepath1,
            bucket2,
            s3filepath2
    ):
        self.s3.copy_object(
            CopySource={
                'Bucket': bucket1,
                'Key': s3filepath1
            },
            Bucket=bucket2,
            Key=s3filepath2
        )
        self.s3.delete_object(Bucket=bucket1, Key=s3filepath1)
        
    def upload_folder(
            self,
            s3folderpath,
            localfolderpath
    ):
        for file in os.listdir(localfolderpath):
            self.upload_file(s3folderpath + file, localfolderpath / file)

def download_calpip_update_data():
    # download the following folders into ../data
    # calpip/*, data/geo/PLSS Township Range California.geojson, data/sections/sections.parquet
    s3 = DataDownloader(BUCKETS['raw'])
    folders_to_download = [
        {
            's3folderpath': 'calpip/',
            'outputfolderpath': DATA_DIR
        },
        {
            's3folderpath': 'census_data/',
            'outputfolderpath': DATA_DIR
        },
        {
            's3folderpath': 'census_geos/crosswalks',
            'outputfolderpath': DATA_DIR
        },
        {
            's3folderpath': 'pur/',
            'outputfolderpath': DATA_DIR
        }
    ]
    files_to_download = [
        {
            's3filepath': 'geo/PLSS Township Range California.geojson',
            'outputfilepath': DATA_DIR / 'geo' / 'PLSS Township Range California.geojson'
        },
        {
            's3filepath': 'sections/sections.parquet',
            'outputfilepath': DATA_DIR / 'sections' / 'sections.parquet'
        },
        {
            's3filepath': 'meta/categories.parquet',
            'outputfilepath': DATA_DIR / 'meta' / 'categories.parquet'
        }
    ]
    print(f"Downloading {len(folders_to_download)} folders")
    for folder in folders_to_download:
        s3.download_folder(**folder)
    print(f"Downloading {len(files_to_download)} files")
    for file in files_to_download:
        s3.download_file(**file)

def upload_clean_calpip_data():
    bucketManager = DataDownloader(BUCKETS['raw'])
    with open(BASE_DIR / f"files_to_remove.txt", 'r') as f:
        files_to_remove = f.readlines()
    for file in files_to_remove:
        try:
            bucketManager.s3.delete_object(Bucket=BUCKETS['raw'], Key=file.strip())
        except Exception as e:
            print(f"Error deleting {file}: {e}")

    existing_files = [row['Key'].split("/")[-1]  for row in bucketManager.s3.list_objects_v2(Bucket=BUCKETS['raw'], Prefix="calpip/")['Contents']]
    # for file in os.listdir(DATA_DIR / 'calpip') if not in bucket / calpip, upload
    parquet_files = [file for file in os.listdir(DATA_DIR / 'calpip') if file.endswith('.parquet')]
    for file in parquet_files:
        if file not in existing_files or file == 'calpip_full.parquet':
            print(f"Uploading {file}")
            bucketManager.upload_file(f"calpip/{file}", DATA_DIR / 'calpip' / file)

def upload_final_outputs():
    s3 = DataDownloader(BUCKETS['output'])
    for fileconfig in OUTPUT_FILENAMES:
        # if file exists copy to backup bucket
        try:
            s3.move_file(BUCKETS['output'], fileconfig['s3filepath'], BUCKETS['output_backup'], fileconfig['s3filepath'])
        except Exception as e:
            print(f"Error moving {fileconfig['s3filepath']} to backup bucket: {e}")
        s3.upload_file(**fileconfig)

def clear_cache():
    # TODO
    pass
