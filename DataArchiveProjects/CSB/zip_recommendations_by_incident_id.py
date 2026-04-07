#!/usr/bin/env python3
"""
Zip CSB Recommendation Files by Incident ID

This script groups CSB recommendation files by their incident ID (the prefix before
the first underscore) and creates separate zip archives for each incident.

Only files starting with 4 digits are processed. Files that don't match this pattern
are logged to a skip log file.

Usage:
    python zip_recommendations_by_incident_id.py [--mode {both,zip,log}]

    --mode both: Create zip archives AND generate CSV log (default)
    --mode zip:  Only create zip archives (no CSV log)
    --mode log:  Only generate CSV log (no zip archives)

Configuration:
    - Source directory: D:\data_archives_2025\CSB\Past\Recommendations
    - Output directory: D:\data_archives_2025\CSB\Past\zips_by_id_for_upload
    - Skip log: zip_script_log.txt (in source directory)
    - CSV log: zip_recommendations_inventory.csv (in output directory)
"""

import os
import zipfile
import re
import csv
import argparse
from collections import defaultdict
from pathlib import Path

# Configuration
SOURCE_DIR = r"D:\data_archives_2025\CSB\Past\Recommendations"
OUTPUT_DIR = r"D:\data_archives_2025\CSB\Past\zips_by_id_for_upload"
LOG_FILE = "zip_script_log.txt"
CSV_LOG = "zip_recommendations_inventory.csv"

def main():
    """Main function to zip recommendation files by incident ID."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Zip CSB recommendation files by incident ID and/or generate inventory CSV'
    )
    parser.add_argument(
        '--mode',
        choices=['both', 'zip', 'log'],
        default='both',
        help='Operation mode: both (zip + CSV), zip (only zips), log (only CSV)'
    )
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize tracking
    incident_files = defaultdict(list)
    skipped_files = []

    # Get all files in source directory
    print(f"Scanning directory: {SOURCE_DIR}")
    files = [f for f in os.listdir(SOURCE_DIR) if os.path.isfile(os.path.join(SOURCE_DIR, f))]

    # Group files by incident ID
    for filename in files:
        # Check if file starts with 4 digits
        if not re.match(r'^\d{4}', filename):
            skipped_files.append(filename)
            continue

        # Extract incident ID (everything before first underscore)
        if '_' in filename:
            incident_id = filename.split('_')[0]
            incident_files[incident_id].append(filename)
        else:
            # File starts with 4 digits but has no underscore
            skipped_files.append(filename)

    # Create zip files if mode is 'both' or 'zip'
    print(f"\nFound {len(incident_files)} unique incident IDs")

    if args.mode in ['both', 'zip']:
        print(f"Creating zip archives...\n")

        for incident_id, files_list in sorted(incident_files.items()):
            # Add _Recommendations suffix to zip filename
            zip_filename = os.path.join(OUTPUT_DIR, f"{incident_id}_Recommendations.zip")

            print(f"Creating {incident_id}_Recommendations.zip ({len(files_list)} files)")

            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for filename in files_list:
                    file_path = os.path.join(SOURCE_DIR, filename)
                    # Add file to zip with just its filename (no directory structure)
                    zipf.write(file_path, arcname=filename)

            print(f"  ✓ Created: {zip_filename}")

    # Create CSV inventory log if mode is 'both' or 'log'
    if args.mode in ['both', 'log']:
        csv_path = os.path.join(OUTPUT_DIR, CSV_LOG)
        print(f"\nGenerating CSV inventory log...")

        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['zip_archive_name', 'file_count', 'files'])

            for incident_id, files_list in sorted(incident_files.items()):
                zip_name = f"{incident_id}_Recommendations.zip"
                file_count = len(files_list)
                files_str = '; '.join(sorted(files_list))
                writer.writerow([zip_name, file_count, files_str])

        print(f"  ✓ Created: {csv_path}")

    # Write skip log
    if skipped_files:
        log_path = os.path.join(SOURCE_DIR, LOG_FILE)
        with open(log_path, 'w') as f:
            f.write(f"Files skipped (did not start with 4 digits or had no underscore):\n")
            f.write(f"Total skipped: {len(skipped_files)}\n")
            f.write("-" * 80 + "\n\n")
            for filename in sorted(skipped_files):
                f.write(f"{filename}\n")

        print(f"\n{len(skipped_files)} files skipped (see {LOG_FILE})")

    # Summary
    print(f"\n{'='*80}")
    print(f"Summary:")
    print(f"  Mode: {args.mode}")
    print(f"  Total files processed: {sum(len(files) for files in incident_files.values())}")
    print(f"  Unique incident IDs: {len(incident_files)}")
    if args.mode in ['both', 'zip']:
        print(f"  Zip files created: {len(incident_files)}")
    if args.mode in ['both', 'log']:
        print(f"  CSV log created: {CSV_LOG}")
    print(f"  Files skipped: {len(skipped_files)}")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
