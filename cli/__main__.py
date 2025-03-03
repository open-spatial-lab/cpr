import sys
import download_sections
import clean_calpip
import intersect_sections
import output_census
import join_census_units
import parse_meta
import utils
import dotenv
dotenv.load_dotenv()

script_to_run = sys.argv[1]

script_dict = {
  "download": utils.download_calpip_update_data,
  "download_geo": download_sections.main,
  "clean": clean_calpip.main,
  "intersect": intersect_sections.main,
  "join": join_census_units.main,
  "output": output_census.main,
  "meta": parse_meta.main,
  "upload_final": utils.upload_final_outputs,
  "upload_clean": utils.upload_clean_calpip_data
}

if __name__ == "__main__":
  print("Running CLI Command ", script_to_run)
  if script_to_run in script_dict:
    script_dict[script_to_run]()
  else: 
    print(f"Unknown script: {script_to_run}")