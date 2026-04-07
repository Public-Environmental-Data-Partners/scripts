#!/bin/bash

# Remember to pipe the output from this script to a file
# It generates a line of output for every file added to
# the zips. That could be useful if something goes wrong 
# but is too long and noisy for the commandline

ORIGINAL_DIR=$(pwd) # Save the original directory
echo "$ORIGINAL_DIR"
DOWNLOAD_DIR="./download" # The directory containing folders to zip
echo "$DOWNLOAD_DIR"

if [ -d "$DOWNLOAD_DIR" ]; then
  cd "$DOWNLOAD_DIR"
  echo "in download dir"
  for folder in year_[0-9][0-9][0-9][0-9]_methodology/; do
    folder_name=$(basename "$folder")
    echo "next folder to zip: $folder_name"
    zip_file="${folder_name}.zip"
    zip -r "$zip_file" "$folder"
    if [ $? -eq 0 ]; then
      echo "Successfully zipped: $folder_name"
    else
      echo "Error zipping: $folder_name"
    fi
  done

  cd "$ORIGINAL_DIR" # Return to the original directory
  echo "Zipping process complete."

else
  echo "Error: Directory '$DOWNLOAD_DIR' not found."
fi
