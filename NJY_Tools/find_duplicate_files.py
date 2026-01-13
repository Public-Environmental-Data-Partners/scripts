"""
Find Duplicate Files by Content
Uses MD5 hash to identify files with identical content across multiple directories
Outputs a CSV file with duplicate file locations

Usage:
    python find_duplicate_files.py --dirs "C:/dir1" "C:/dir2" "C:/dir3"
    python find_duplicate_files.py -d "C:/dir1" "C:/dir2"
"""

import os
import hashlib
import sys
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def calculate_file_hash(file_path):
    """
    Calculate MD5 hash of a file's content

    Args:
        file_path: Path to file

    Returns:
        str: MD5 hash of file content
    """
    md5_hash = hashlib.md5()

    try:
        with open(file_path, 'rb') as f:
            # Read file in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def find_duplicates(directories):
    """
    Find all duplicate files across multiple directories

    Args:
        directories: List of directory paths to scan

    Returns:
        dict: Hash -> list of (file_path, directory) tuples
    """
    hash_map = defaultdict(list)
    total_files = 0

    # Scan each directory
    for dir_path in directories:
        print(f"\nScanning: {dir_path}")

        files = [f for f in Path(dir_path).iterdir() if f.is_file()]
        print(f"  Found {len(files)} files")

        # Calculate hash for each file
        for idx, file_path in enumerate(files, 1):
            file_hash = calculate_file_hash(file_path)

            if file_hash:
                hash_map[file_hash].append((str(file_path), dir_path))
                total_files += 1

            # Progress indicator
            if idx % 10 == 0:
                print(f"    Processed {idx}/{len(files)} files...", end='\r')

        print(f"    Processed {len(files)}/{len(files)} files... Done!")

    print(f"\nTotal files scanned: {total_files}")

    # Filter to only duplicates (hash appears more than once)
    duplicates = {h: paths for h, paths in hash_map.items() if len(paths) > 1}

    return duplicates


def format_file_size(size_bytes):
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def main():
    """Main function"""
    # Parse command line
    if len(sys.argv) < 2 or sys.argv[1] in ['--help', '-h']:
        print(__doc__)
        print("\nExamples:")
        print('  python find_duplicate_files.py --dirs "C:/folder1" "C:/folder2"')
        print('  python find_duplicate_files.py -d "C:/data" "D:/backup"')
        return

    directories = []

    if sys.argv[1] in ['--dirs', '-d']:
        # Everything after --dirs is a directory
        directories = sys.argv[2:]
    else:
        # All arguments are directories
        directories = sys.argv[1:]

    # Validate directories
    valid_directories = []
    for directory in directories:
        if os.path.isdir(directory):
            valid_directories.append(directory)
        else:
            print(f"Warning: Directory not found (skipping): {directory}")

    if not valid_directories:
        print("Error: No valid directories specified")
        return

    if len(valid_directories) > 15:
        print(f"Warning: Maximum 15 directories supported. Using first 15.")
        valid_directories = valid_directories[:15]

    print("=" * 70)
    print("Duplicate File Finder (Multi-Directory)")
    print("=" * 70)
    print(f"Scanning {len(valid_directories)} directories:")
    for i, d in enumerate(valid_directories, 1):
        print(f"  {i}. {d}")
    print("=" * 70)

    # Find duplicates
    duplicates = find_duplicates(valid_directories)

    # Display results
    print()
    print("=" * 70)
    print("Results")
    print("=" * 70)

    if not duplicates:
        print("✓ No duplicate files found!")
        print("  All files have unique content")
    else:
        total_duplicates = sum(len(paths) - 1 for paths in duplicates.values())
        print(f"⚠️  Found {len(duplicates)} sets of duplicate files")
        print(f"   ({total_duplicates} duplicate files total)")
        print()

        # Prepare CSV data
        csv_rows = []
        max_duplicates = 0

        for file_hash, paths in duplicates.items():
            # Sort paths, prioritizing "uploaded" folders first
            def sort_key(item):
                file_path, dir_path = item
                # Check if "uploaded" is anywhere in the path (case-insensitive)
                has_uploaded = 'uploaded' in file_path.lower()
                return (not has_uploaded, file_path)  # False sorts before True, so uploaded comes first

            sorted_paths = sorted(paths, key=sort_key)
            max_duplicates = max(max_duplicates, len(sorted_paths))

            # Create row: [first_location, second_location, third_location, ...]
            row = [path for path, _ in sorted_paths]
            csv_rows.append(row)

        # Save CSV and delete duplicates
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"duplicate_files_{timestamp}.csv"
        deleted_files = []
        deletion_errors = []

        # Save CSV BEFORE deleting files
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # Header row
            headers = ['Original (or Uploaded folder)', 'Status'] + [f'Duplicate {i}' for i in range(1, max_duplicates)]
            writer.writerow(headers)

            # Data rows
            for row in csv_rows:
                # Row format: [kept_file, "KEPT", deleted_file1, deleted_file2, ...]
                csv_row = [row[0], 'KEPT'] + row[1:]
                padded_row = csv_row + [''] * (max_duplicates + 1 - len(csv_row))
                writer.writerow(padded_row)

        print(f"\n📄 CSV file saved: {csv_filename}")

        # Now delete duplicate files (NOT in "uploaded" folder)
        print()
        print("=" * 70)
        print("DELETING DUPLICATE FILES")
        print("=" * 70)

        for idx, row in enumerate(csv_rows, 1):
            kept_file = row[0]
            duplicates_to_delete = row[1:]

            print(f"\nSet #{idx}: Keeping {os.path.basename(kept_file)}")

            for dup_path in duplicates_to_delete:
                try:
                    os.remove(dup_path)
                    deleted_files.append(dup_path)
                    print(f"  ✓ Deleted: {dup_path}")
                except Exception as e:
                    deletion_errors.append((dup_path, str(e)))
                    print(f"  ✗ Failed to delete: {dup_path}")
                    print(f"    Error: {e}")

        # Summary
        print()
        print("=" * 70)
        print("DELETION SUMMARY")
        print("=" * 70)
        print(f"Files kept (in 'uploaded' or first occurrence): {len(csv_rows)}")
        print(f"Files deleted successfully: {len(deleted_files)}")
        print(f"Files failed to delete: {len(deletion_errors)}")

        # Calculate space freed
        space_freed = sum(os.path.getsize(csv_rows[i][0]) for i in range(len(csv_rows))) * (len(deleted_files) / max(len(csv_rows), 1))
        if deleted_files:
            # Calculate based on first file in each set
            space_freed = sum(
                os.path.getsize(paths[0][0]) * (len(paths) - 1)
                for paths in duplicates.values()
            )
            print(f"💾 Space freed: {format_file_size(space_freed)}")

        print(f"\n📄 Full record saved in: {csv_filename}")
        print("=" * 70)


if __name__ == "__main__":
    main()
