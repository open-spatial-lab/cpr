#!/bin/bash
python3 cli download
python3 cli clean
python3 cli upload_clean
python3 cli meta
python3 cli join
python3 cli upload_final

echo "CalPIP data update process completed successfully."
