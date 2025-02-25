# %%
import pandas as pd
import requests
import geopandas as gpd
from glob import glob
# bs4
from bs4 import BeautifulSoup
from os import path, remove
# %%
data_dir = path.join(path.dirname(__file__), "..", "data")
# %%
def get_url(path:str) -> str:
  return f"https://www.cdpr.ca.gov/{path}"

def download_file(url, outpath):
  file_bytes = requests.get(url).content
  with open(outpath, 'wb') as f:
    f.write(file_bytes)

def download_all():
  root_url = "https://calpip.cdpr.ca.gov"
  page = f"{root_url}/plssFiles.cfm"
  print(f"Downloading {page}")
  html = requests.get(page).content
  soup = BeautifulSoup(html, 'html.parser')

  links = soup.find_all('a', href=lambda x: x and 'County_PLSS' in x)
  print(f"Found {len(links)} links")
  for link in links:
    print(f"Downloading {link['href']}")
    name = link['href'].split('/')[-1]
    outpath =  path.join(data_dir, "sections", name)
    download_file(f"{root_url}{link['href']}", outpath)

def concat_gdfs():
  files = glob(path.join(data_dir, "sections", "*.zip"))
  print(f"Downloaded {len(files)} files")
  gdf = pd.concat([gpd.read_file(f) for f in files])
  gdf = gdf.to_crs("EPSG:4326")
  # dissolve by CO_MTRS
  gdf = gdf.dissolve(by='CO_MTRS').reset_index()
  gdf.to_parquet(path.join(data_dir, "sections", "sections.parquet"))
  gdf.to_file(path.join(data_dir, "sections", "sections.geojson"), driver='GeoJSON')

def clean_up_zips():
  files = glob(path.join(data_dir, "sections", "*.zip"))
  for f in files:
    print(f"Removing {f}")
    remove(f)
# %%
def main():
  print("Downloading sections")
  download_all() 
  concat_gdfs()
  clean_up_zips()

if __name__ == "__main__":
  main()