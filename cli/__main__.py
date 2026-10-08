import sys
import importlib

# pipeline (see .github/workflows/build-data.yml): clean -> meta -> build_r2, then compare against the live build
# static geography prep, rerun only when boundaries or ACS data change: download_geo, intersect, output
COMMANDS = {
  "clean": "clean_calpip",
  "meta": "parse_meta",
  "build_r2": "build_r2",
  "compare": "compare_builds",
  "download_geo": "download_sections",
  "intersect": "intersect_sections",
  "output": "output_census",
}

if __name__ == "__main__":
  command = sys.argv[1] if len(sys.argv) > 1 else ""
  if command not in COMMANDS:
    sys.exit(f"Unknown command: {command!r}. One of: {', '.join(COMMANDS)}")
  print("Running CLI Command", command, file=sys.stderr)  # stderr: `compare` output goes to the job summary
  # import only the step being run, so the pipeline doesn't need the geo steps' dependencies
  importlib.import_module(COMMANDS[command]).main()
