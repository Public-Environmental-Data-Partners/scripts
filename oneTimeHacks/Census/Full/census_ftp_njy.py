"""
Simple Census FTP Downloader
- Reads from catalog CSV
- Only downloads real FTP files (www2.census.gov)
- Skips files that already exist
- Creates directories as needed
- Logs downloads to a clean catalog
- Resumable (just run again to continue)
"""

import os
import csv
import requests
import time
from datetime import datetime

# Config
CATALOG_CSV = r"F:\data_archives_2025\Census_all\Census_ftp_Downloads\census_ftp_catalog.csv"
DOWNLOAD_DIR = r"F:\data_archives_2025\Census_all\Census_ftp_Downloads"
DOWNLOAD_LOG = r"F:\data_archives_2025\Census_all\Census_ftp_Downloads\census_ftp_downloaded.csv"
RATE_LIMIT = 0.2  # seconds between downloads
TIMEOUT = 60  # seconds

# Required to avoid 403 Forbidden from Census servers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


def download_file(url, local_path):
    """Download a single file, return file size if successful"""
    try:
        response = requests.get(url, timeout=TIMEOUT, stream=True, headers=HEADERS)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return os.path.getsize(local_path)
        else:
            print(f"  HTTP {response.status_code}: {url[:70]}")
    except Exception as e:
        print(f"  ERROR: {e}")
    return None


def select_categories(files):
    """Let user select which root directories to download"""
    # Count files per category
    categories = {}
    for f in files:
        cat = f['category']
        if cat not in categories:
            categories[cat] = 0
        categories[cat] += 1

    # Sort by name
    sorted_cats = sorted(categories.items())

    print("\n" + "="*60)
    print("SELECT DIRECTORIES TO DOWNLOAD")
    print("="*60)
    print("\nAvailable root directories:\n")

    for i, (cat, count) in enumerate(sorted_cats, 1):
        print(f"  {i:2}. {cat:40} ({count:,} files)")

    print(f"\n  A. ALL directories")
    print(f"  Q. Quit")

    choice = input("\nEnter numbers separated by commas (e.g., 1,3,5) or A for all: ").strip().upper()

    if choice == 'Q':
        return []
    elif choice == 'A':
        return [cat for cat, _ in sorted_cats]
    else:
        try:
            indices = [int(n.strip()) - 1 for n in choice.split(',')]
            return [sorted_cats[i][0] for i in indices if 0 <= i < len(sorted_cats)]
        except:
            print("Invalid selection")
            return []


def main():
    print("="*60)
    print("CENSUS FTP DOWNLOADER")
    print("="*60)
    print(f"Download directory: {DOWNLOAD_DIR}")
    print(f"Download log: {DOWNLOAD_LOG}")

    # Load catalog first
    print(f"\nLoading catalog: {CATALOG_CSV}")
    with open(CATALOG_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_files = list(reader)

    print(f"Found {len(all_files):,} entries in catalog")

    # Filter to only real FTP files (www2.census.gov)
    files = [f for f in all_files if 'www2.census.gov' in f['url']]
    print(f"Filtered to {len(files):,} real FTP files (www2.census.gov)")

    # Let user select categories
    selected_cats = select_categories(files)
    if not selected_cats:
        print("No directories selected. Exiting.")
        return

    # Filter files to selected categories
    files = [f for f in files if f['category'] in selected_cats]
    print(f"\nSelected {len(files):,} files from {len(selected_cats)} directories")

    # Stats
    skipped = 0
    downloaded = 0
    failed = 0
    dirs_created = 0

    start_time = datetime.now()

    # Open download log (append mode so we can resume)
    log_exists = os.path.exists(DOWNLOAD_LOG)
    log_file = open(DOWNLOAD_LOG, 'a', newline='', encoding='utf-8')
    fieldnames = ['path', 'name', 'url', 'size_bytes', 'extension', 'category', 'downloaded_at']
    log_writer = csv.DictWriter(log_file, fieldnames=fieldnames)
    if not log_exists:
        log_writer.writeheader()

    try:
        for i, file_info in enumerate(files):
            url = file_info['url']
            rel_path = file_info['path']

            # Local path
            local_path = os.path.join(DOWNLOAD_DIR, rel_path)
            local_dir = os.path.dirname(local_path)

            # Progress every 100 files
            if i % 100 == 0:
                print(f"\n[{i:,}/{len(files):,}] Downloaded: {downloaded} | Skipped: {skipped} | Failed: {failed} | Dirs created: {dirs_created}")

            # Skip if file exists
            if os.path.exists(local_path):
                skipped += 1
                continue

            # Create directory if needed
            if not os.path.exists(local_dir):
                os.makedirs(local_dir, exist_ok=True)
                dirs_created += 1

            # Download
            print(f"  Downloading: {rel_path}")
            file_size = download_file(url, local_path)

            if file_size is not None:
                downloaded += 1
                # Log successful download
                log_writer.writerow({
                    'path': rel_path,
                    'name': file_info['name'],
                    'url': url,
                    'size_bytes': file_size,
                    'extension': file_info.get('extension', ''),
                    'category': file_info['category'],
                    'downloaded_at': datetime.now().isoformat()
                })
                log_file.flush()
            else:
                failed += 1

            time.sleep(RATE_LIMIT)

    finally:
        log_file.close()

    # Summary
    elapsed = datetime.now() - start_time
    print(f"\n{'='*60}")
    print("DOWNLOAD COMPLETE")
    print("="*60)
    print(f"Downloaded: {downloaded:,}")
    print(f"Skipped (already had): {skipped:,}")
    print(f"Directories created: {dirs_created:,}")
    print(f"Failed: {failed:,}")
    print(f"Time: {elapsed}")
    print(f"\nDownload log saved to: {DOWNLOAD_LOG}")


if __name__ == "__main__":
    main()
