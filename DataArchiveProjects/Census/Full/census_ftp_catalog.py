"""
Census FTP Site Catalog - Scrapes the Census FTP site to catalog all available files
Creates a comprehensive inventory of files, sizes, and folder structure
"""

import os
import csv
import json
import time
import requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import defaultdict

# Base FTP URL (accessed via HTTP)
FTP_BASE = "https://www2.census.gov/"

# Output files
OUTPUT_DIR = r"D:\data_archives_2025"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "census_ftp_catalog.csv")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "census_ftp_catalog.json")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "ftp_scrape_progress.json")

# Incremental file for saving as we go (prevents data loss on Ctrl+C)
INCREMENTAL_CSV = os.path.join(OUTPUT_DIR, "census_ftp_catalog_incremental.csv")

# Rate limiting
RATE_LIMIT = 0.3

# Maximum depth to crawl (to avoid going too deep)
MAX_DEPTH = 4

# File extensions to catalog (None = all files)
INCLUDE_EXTENSIONS = None  # Set to ['.csv', '.zip', '.xlsx'] to filter

# Directories to skip (large or not useful)
SKIP_DIRS = [
    'programs-surveys/acs/data/pums',  # Very large microdata
]


def load_progress():
    """Load progress from previous run"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {
        'visited_dirs': [],
        'files_found': 0,
        'last_dir': ''
    }


def save_progress(progress):
    """Save progress for resume"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def parse_ftp_listing(html, base_url):
    """Parse the FTP directory listing HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    items = []

    # Find all links in the directory listing
    for link in soup.find_all('a'):
        href = link.get('href', '')
        name = link.get_text().strip()

        # Skip parent directory and empty links
        if not href or href == '../' or name in ['Parent Directory', 'Name', '']:
            continue

        # Skip sorting links
        if href.startswith('?'):
            continue

        # Determine if it's a directory or file
        is_dir = href.endswith('/')

        # Get the full URL
        full_url = urljoin(base_url, href)

        # Try to get size from the listing (if available)
        size = None
        parent = link.find_parent('tr')
        if parent:
            cells = parent.find_all('td')
            for cell in cells:
                text = cell.get_text().strip()
                # Look for size patterns like "1.2M", "456K", "789"
                if text and text[0].isdigit():
                    size = text

        items.append({
            'name': name.rstrip('/'),
            'url': full_url,
            'is_dir': is_dir,
            'size': size
        })

    return items


def get_directory_listing(url, retries=3):
    """Fetch and parse a directory listing"""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return parse_ftp_listing(response.text, url)
            elif response.status_code == 404:
                return []
        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}")
        except Exception as e:
            print(f"  Error: {e}")

        time.sleep(RATE_LIMIT * 2)

    return []


def should_skip_dir(path):
    """Check if directory should be skipped"""
    for skip in SKIP_DIRS:
        if skip in path:
            return True
    return False


def crawl_directory(url, path="", depth=0, progress=None, all_files=None, csv_writer=None):
    """Recursively crawl a directory"""
    if all_files is None:
        all_files = []

    if depth > MAX_DEPTH:
        return all_files

    if should_skip_dir(path):
        print(f"  Skipping: {path}")
        return all_files

    # Check if already visited
    if progress and path in progress['visited_dirs']:
        return all_files

    print(f"{'  ' * depth}[{depth}] {path or 'ROOT'}")

    items = get_directory_listing(url)
    time.sleep(RATE_LIMIT)

    for item in items:
        item_path = f"{path}/{item['name']}" if path else item['name']

        if item['is_dir']:
            # Recursively crawl subdirectory
            crawl_directory(item['url'], item_path, depth + 1, progress, all_files, csv_writer)
        else:
            # Add file to catalog
            ext = os.path.splitext(item['name'])[1].lower()

            # Filter by extension if specified
            if INCLUDE_EXTENSIONS and ext not in INCLUDE_EXTENSIONS:
                continue

            file_info = {
                'path': item_path,
                'name': item['name'],
                'url': item['url'],
                'size': item['size'],
                'extension': ext,
                'category': path.split('/')[0] if path else '',
                'depth': depth
            }
            all_files.append(file_info)

            # Write to incremental CSV immediately (prevents data loss)
            if csv_writer:
                csv_writer.writerow(file_info)

            if len(all_files) % 100 == 0:
                print(f"    Found {len(all_files)} files...")

    # Mark directory as visited
    if progress:
        progress['visited_dirs'].append(path)
        progress['files_found'] = len(all_files)
        progress['last_dir'] = path

        # Save progress periodically
        if len(progress['visited_dirs']) % 50 == 0:
            save_progress(progress)

    return all_files


def get_top_level_dirs():
    """Get just the top-level directories for selection"""
    print("Fetching top-level directories...")
    items = get_directory_listing(FTP_BASE)

    dirs = [item for item in items if item['is_dir']]
    return dirs


def select_directories(dirs):
    """Let user select which directories to crawl"""
    print("\n" + "="*60)
    print("SELECT DIRECTORIES TO CATALOG")
    print("="*60)
    print("\nAvailable top-level directories:\n")

    for i, d in enumerate(dirs, 1):
        print(f"  {i:2}. {d['name']}")

    print(f"\n  A. ALL directories")
    print(f"  Q. Quit")

    choice = input("\nEnter numbers separated by commas (e.g., 1,3,5) or A for all: ").strip().upper()

    if choice == 'Q':
        return []
    elif choice == 'A':
        return dirs
    else:
        try:
            indices = [int(n.strip()) - 1 for n in choice.split(',')]
            return [dirs[i] for i in indices if 0 <= i < len(dirs)]
        except:
            print("Invalid selection")
            return []


def main():
    print("="*60)
    print("CENSUS FTP SITE CATALOG")
    print("="*60)
    print(f"Base URL: {FTP_BASE}")
    print(f"Max depth: {MAX_DEPTH}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load progress
    progress = load_progress()

    if progress['files_found'] > 0:
        print(f"\nFound previous progress: {progress['files_found']} files cataloged")
        print(f"Last directory: {progress['last_dir']}")
        resume = input("Resume from where you left off? (yes/no): ").strip().lower()
        if resume != 'yes':
            progress = {
                'visited_dirs': [],
                'files_found': 0,
                'last_dir': ''
            }

    # Get top-level directories
    top_dirs = get_top_level_dirs()

    if not top_dirs:
        print("ERROR: Could not fetch directory listing")
        return

    # Let user select directories
    selected_dirs = select_directories(top_dirs)

    if not selected_dirs:
        print("No directories selected. Exiting.")
        return

    print(f"\nWill catalog {len(selected_dirs)} directories")
    print("This may take a while depending on directory sizes...")

    confirm = input("\nStart cataloging? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Aborted.")
        return

    # Crawl selected directories
    all_files = []
    start_time = datetime.now()

    # Open incremental CSV for writing (append if resuming, write if fresh)
    fieldnames = ['path', 'name', 'url', 'size', 'extension', 'category', 'depth']
    csv_mode = 'a' if progress['files_found'] > 0 and os.path.exists(INCREMENTAL_CSV) else 'w'

    print(f"\nSaving files incrementally to: {INCREMENTAL_CSV}")

    with open(INCREMENTAL_CSV, csv_mode, newline='', encoding='utf-8') as inc_f:
        csv_writer = csv.DictWriter(inc_f, fieldnames=fieldnames)

        # Write header only if starting fresh
        if csv_mode == 'w':
            csv_writer.writeheader()

        for d in selected_dirs:
            print(f"\n{'='*60}")
            print(f"Cataloging: {d['name']}")
            print("="*60)

            crawl_directory(d['url'], d['name'], depth=1, progress=progress, all_files=all_files, csv_writer=csv_writer)
            save_progress(progress)
            inc_f.flush()  # Flush after each top-level directory

    elapsed = datetime.now() - start_time

    # Copy incremental to final CSV
    print(f"\nCopying to final CSV: {OUTPUT_CSV}")
    import shutil
    shutil.copy(INCREMENTAL_CSV, OUTPUT_CSV)

    # Save to JSON
    print(f"Saving to JSON: {OUTPUT_JSON}")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(all_files, f, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print("CATALOG SUMMARY")
    print("="*60)
    print(f"Total files found: {len(all_files):,}")
    print(f"Time elapsed: {elapsed}")

    # By extension
    ext_counts = defaultdict(int)
    for f in all_files:
        ext_counts[f['extension']] += 1

    print(f"\nFiles by extension:")
    for ext, count in sorted(ext_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {ext or '(none)':10} {count:,}")

    # By category
    cat_counts = defaultdict(int)
    for f in all_files:
        cat_counts[f['category']] += 1

    print(f"\nFiles by category:")
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:30} {count:,}")

    print(f"\nFiles saved to:")
    print(f"  - {OUTPUT_CSV}")
    print(f"  - {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
