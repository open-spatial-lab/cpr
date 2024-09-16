import sys
import download_sections
import clean_calpip
import intersect_sections
import output_census

script_to_run = sys.argv[1]

script_dict = {
  "download_sections": download_sections.main,
  "clean_calpip": clean_calpip.main,
  "intersect_sections": intersect_sections.main,
  "output_census": output_census.main,
}

if script_to_run in script_dict:
  script_dict[script_to_run]()
else: 
  print(f"Unknown script: {script_to_run}")