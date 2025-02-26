import sys
import download_sections
import clean_calpip
import intersect_sections
import output_census
import join_census_units
import parse_meta
import utils

script_to_run = sys.argv[1]

script_dict = {
  "download_calpip": utils.download_calpip_update_data,
  "download_sections": download_sections.main,
  "clean_calpip": clean_calpip.main,
  "intersect_sections": intersect_sections.main,
  "join_census_units": join_census_units.main,
  "output_census": output_census.main,
  "parse_meta": parse_meta.main,
  "upload_final_outputs": utils.upload_final_outputs,
  "upload_clean_calpip_data": utils.upload_clean_calpip_data
}

if __name__ == "__main__":
  if script_to_run in script_dict:
    script_dict[script_to_run]()
  else: 
    print(f"Unknown script: {script_to_run}")