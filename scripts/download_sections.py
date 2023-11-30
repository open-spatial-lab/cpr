# %%
import pandas as pd
import wget
import geopandas as gpd
from glob import glob
# bs4
from bs4 import BeautifulSoup
# %%
def get_url(path:str) -> str:
  return f"https://www.cdpr.ca.gov/{path}"

def download_all():
  page = "https://www.cdpr.ca.gov/docs/emon/grndwtr/plss_shapefiles.htm"
  html = wget.download(page)
  with open(html) as fp:
    soup = BeautifulSoup(fp, 'html.parser')
  links = soup.find_all('a', href=lambda x: x and 'County_PLSS' in x)
  for link in links:
    url = get_url(link['href'])
    name = link['href'].split('/')[-1]
    wget.download(url, f"../data/sections/{name}")

def concat_gdfs():
  files  = glob("../data/sections/*.zip")
  gdf = pd.concat([gpd.read_file(f) for f in files])
  gdf = gdf.to_crs("EPSG:4326")
  gdf.to_parquet("../data/sections/sections.parquet")
  gdf.to_file("../data/sections/sections.geojson", driver='GeoJSON')
