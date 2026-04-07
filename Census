"""
Simple Census FTP Downloader
- Reads from catalog CSV
- Only downloads real FTP files (www2.census.gov)
- Skips files that already exist
- Creates directories as needed
- Logs downloads to a clean catalog
- Resumable (just run again to continue)
- Uses concurrent downloads for speed
"""

import os
import csv
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Config
CATALOG_CSV = r"F:\data_archives_2025\Census_all\Census_ftp_Downloads\census_ftp_catalog.csv"
DOWNLOAD_DIR = r"F:\data_archives_2025\Census_all\Census_ftp_Downloads"
DOWNLOAD_LOG = r"F:\data_archives_2025\Census_all\Census_ftp_Downloads\census_ftp_downloaded.csv"
WORKERS = 12  # concurrent download threads
TIMEOUT = (30, 300)  # (connect timeout, read timeout) - 30s to connect, 5 min to download
CHUNK_SIZE = 131072  # 128KB chunks

# Browser-like headers to avoid 403 Forbidden from Census servers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www2.census.gov/',
    'Connection': 'keep-alive',
}

# Reusable session for connection pooling (one per thread)
_thread_local = threading.local()
_shutdown = threading.Event()  # signals threads to stop


def get_session():
    """Get a per-thread requests session for connection reuse"""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def download_file(url, local_path):
    """Download a single file, return file size if successful.
    Downloads to a .tmp file first, then renames on success to avoid partial files."""
    if _shutdown.is_set():
        return None
    tmp_path = local_path + '.tmp'
    try:
        session = get_session()
        response = session.get(url, timeout=TIMEOUT, stream=True)
        if response.status_code == 200:
            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
            os.replace(tmp_path, local_path)
            return os.path.getsize(local_path)
        else:
            print(f"  HTTP {response.status_code}: {url[:70]}")
    except Exception as e:
        print(f"  ERROR: {e}")
    # Clean up partial .tmp file on failure
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
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
    total_bytes = 0
    log_lock = threading.Lock()

    start_time = datetime.now()

    # Pre-filter: skip existing files and create dirs (single-threaded, fast)
    to_download = []
    for file_info in files:
        rel_path = file_info['path']
        local_path = os.path.join(DOWNLOAD_DIR, rel_path)
        local_dir = os.path.dirname(local_path)

        if os.path.exists(local_path):
            skipped += 1
            continue

        if not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)
            dirs_created += 1

        to_download.append((file_info, local_path))

    print(f"Skipped {skipped:,} existing files")
    print(f"Created {dirs_created:,} new directories")
    print(f"Downloading {len(to_download):,} files with {WORKERS} threads...\n")

    # Open download log (append mode so we can resume)
    log_exists = os.path.exists(DOWNLOAD_LOG)
    log_file = open(DOWNLOAD_LOG, 'a', newline='', encoding='utf-8')
    fieldnames = ['path', 'name', 'url', 'size_bytes', 'extension', 'category', 'downloaded_at']
    log_writer = csv.DictWriter(log_file, fieldnames=fieldnames)
    if not log_exists:
        log_writer.writeheader()

    def process_file(file_info, local_path):
        """Download a single file and return result"""
        url = file_info['url']
        file_size = download_file(url, local_path)
        return file_info, local_path, file_size

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(process_file, fi, lp): fi
                for fi, lp in to_download
            }

            try:
                for i, future in enumerate(as_completed(futures)):
                    file_info, local_path, file_size = future.result()

                    if file_size is not None:
                        downloaded += 1
                        total_bytes += file_size
                        with log_lock:
                            log_writer.writerow({
                                'path': file_info['path'],
                                'name': file_info['name'],
                                'url': file_info['url'],
                                'size_bytes': file_size,
                                'extension': file_info.get('extension', ''),
                                'category': file_info['category'],
                                'downloaded_at': datetime.now().isoformat()
                            })
                            log_file.flush()
                    else:
                        failed += 1

                    # Progress every 100 files
                    if (i + 1) % 100 == 0 or (i + 1) == len(to_download):
                        elapsed_so_far = (datetime.now() - start_time).total_seconds()
                        rate_mb = (total_bytes / 1048576) / elapsed_so_far if elapsed_so_far > 0 else 0
                        last_file = file_info['path'].split('/')[-1][:40]
                        print(f"[{i+1:,}/{len(to_download):,}] OK: {downloaded:,} | Failed: {failed:,} | {total_bytes/1073741824:.1f} GB | {rate_mb:.1f} MB/s | {last_file}")

            except KeyboardInterrupt:
                print("\n\nCtrl+C detected - shutting down (waiting for active downloads to finish)...")
                _shutdown.set()
                for f in futures:
                    f.cancel()
                executor.shutdown(wait=True)

    finally:
        log_file.close()

    # Summary
    elapsed = datetime.now() - start_time
    print(f"\n{'='*60}")
    print("DOWNLOAD COMPLETE")
    print("="*60)
    print(f"Downloaded: {downloaded:,}")
    print(f"Total size: {total_bytes/1073741824:.2f} GB")
    print(f"Skipped (already had): {skipped:,}")
    print(f"Directories created: {dirs_created:,}")
    print(f"Failed: {failed:,}")
    print(f"Time: {elapsed}")
    print(f"\nDownload log saved to: {DOWNLOAD_LOG}")


if __name__ == "__main__":
    main()
