#!/bin/bash

# script to consolidate individual csv files into zip archives
# by subpart code

count=0

# Loop over all files with an underscore
for file in *_*; do
  # Skip if not a regular file
  [ -f "$file" ] || continue

  # Extract subpart prefix (before first underscore)
  prefix="${file%%_*}"

  # Add the file to the corresponding zip archive
  zip -q "subpart_${prefix}.zip" "$file"

  # Increment counter
  count=$((count + 1))
done

# Final report
echo "Processed $count files into subpart_*.zip archives."
