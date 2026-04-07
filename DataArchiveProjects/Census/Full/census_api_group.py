"""
Census GROUPED Table Downloader
Downloads datasets with table groups (ACS, Decennial, etc.) from Census API

Features:
- Downloads table groups (B01001, B01002, etc.)
- Resume from where it left off (Ctrl+C safe)
- Choose geography levels
- Data dictionary generation

Requires: pip install requests pandas
"""

import os
import requests
import pandas as pd
import json
import time
import csv
import sys
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

# Your Census API key
API_KEY = "[INSERT YOUR API KEY HERE]"

# Path to the catalog file (xlsx or csv)
CATALOG_FILE = r"[INSERT YOUR PATHNAME AND FILENAME HERE]"

# Base output directory (same as flat script)
OUTPUT_DIR = r"[INSERT YOUR DOWNLOAD DIRECTORY HERE]"

# Census API base URL
API_BASE = "https://api.census.gov/data"

# Rate limiting (seconds between requests)
RATE_LIMIT = 0.3

# Maximum retries per request
MAX_RETRIES = 3

# Progress file (for resume functionality) - separate from flat
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "download_progress_grouped.json")

# Log file - separate from flat
LOG_FILE = os.path.join(OUTPUT_DIR, "download_log_grouped.csv")

# Master index file - separate from flat
INDEX_FILE = os.path.join(OUTPUT_DIR, "download_index_grouped.csv")

# Max variables per API request
MAX_VARS_PER_REQUEST = 50

# Geography levels in order of granularity (most granular first)
# Format: (for_clause, in_clause or None, label, description)
GEOGRAPHY_LEVELS = [
    ('block:*', 'state:*&in=county:*&in=tract:*', 'block', 'Most granular - Census blocks'),
    ('block group:*', 'state:*&in=county:*&in=tract:*', 'block_group', 'Block groups'),
    ('tract:*', 'state:*&in=county:*', 'tract', 'Census tracts'),
    ('county:*', 'state:*', 'county_by_state', 'Counties (with state)'),
    ('county:*', None, 'county', 'Counties'),
    ('state:*', None, 'state', 'States'),
    ('us:*', None, 'us', 'National total'),
]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def load_catalog(catalog_path):
    """Load the catalog file (xlsx or csv)"""
    if catalog_path.endswith('.xlsx'):
        df = pd.read_excel(catalog_path)
    else:
        df = pd.read_csv(catalog_path)

    # Filter to available datasets only
    df = df[df['is_available'].astype(str).str.lower() == 'true']

    # Filter to GROUPED datasets only (table_count > 0)
    df = df[df['table_count'] > 0]

    print(f"Loaded {len(df)} available GROUPED datasets from catalog")
    return df


def get_categories(df):
    """Extract unique dataset categories from the catalog"""
    categories = {}
    for _, row in df.iterrows():
        path = row.get('dataset_path', '')
        if path:
            cat = path.split('/')[0]
            if cat not in categories:
                categories[cat] = {'count': 0, 'tables': 0}
            categories[cat]['count'] += 1
            categories[cat]['tables'] += row.get('table_count', 0) or 0
    return categories


def select_categories(df):
    """Let user select which dataset categories to download"""
    categories = get_categories(df)

    print("\n" + "="*60)
    print("SELECT DATASET CATEGORIES")
    print("="*60)
    print("\nAvailable categories:\n")

    sorted_cats = sorted(categories.items(), key=lambda x: -x[1]['count'])

    for i, (cat, info) in enumerate(sorted_cats, 1):
        print(f"  {i:2}. {cat:25} ({info['count']:4} datasets, {info['tables']:,} tables)")

    print(f"\n  A. ALL categories")
    print(f"  Enter numbers separated by commas (e.g., 1,3,5)")

    choice = input("\nSelect categories: ").strip().upper()

    if choice == 'A' or choice == '':
        print("Selected: ALL categories")
        return df

    try:
        indices = [int(n.strip()) - 1 for n in choice.split(',')]
        selected_cats = [sorted_cats[i][0] for i in indices if 0 <= i < len(sorted_cats)]

        if not selected_cats:
            print("Invalid selection, using all categories")
            return df

        def matches_category(path):
            if not path:
                return False
            cat = path.split('/')[0]
            return cat in selected_cats

        filtered_df = df[df['dataset_path'].apply(matches_category)]
        print(f"\nSelected categories: {', '.join(selected_cats)}")
        print(f"Filtered to {len(filtered_df)} datasets")
        return filtered_df

    except KeyboardInterrupt:
        raise
    except Exception:
        print("Invalid selection, using all categories")
        return df


def scan_existing_files(output_dir):
    """Scan output directory to find already downloaded files and rebuild progress"""
    print("Scanning existing files to rebuild progress...")

    completed_datasets = set()
    completed_tables = set()
    total_downloaded = 0

    if not os.path.exists(output_dir):
        return {
            'completed_datasets': [],
            'completed_tables': [],
            'total_downloaded': 0,
        }

    # Walk through all directories
    for root, dirs, files in os.walk(output_dir):
        csv_files = [f for f in files if f.endswith('.csv') and not f.startswith('_')]

        if not csv_files:
            continue

        # Parse the path to get dataset info
        rel_path = os.path.relpath(root, output_dir)
        parts = rel_path.split(os.sep)

        if len(parts) >= 2:
            category = parts[0]
            try:
                year = int(parts[1])
            except ValueError:
                continue

            # Reconstruct dataset_path
            if len(parts) > 2:
                dataset_path = category + '/' + '/'.join(parts[2:])
            else:
                dataset_path = category

            dataset_key = f"{dataset_path}/{year}"

            for csv_file in csv_files:
                total_downloaded += 1
                # Extract table name
                table_name = csv_file.rsplit('_', 2)[0] if '_' in csv_file else csv_file.replace('.csv', '')
                table_key = f"{dataset_key}/{table_name}"
                completed_tables.add(table_key)
                completed_datasets.add(dataset_key)

    print(f"  Found {len(completed_datasets)} datasets with files")
    print(f"  Found {total_downloaded} table files")

    return {
        'completed_datasets': list(completed_datasets),
        'completed_tables': list(completed_tables),
        'total_downloaded': total_downloaded,
    }


def load_progress():
    """Load progress from previous run"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
                else:
                    print("Warning: Progress file is empty")
        except json.JSONDecodeError:
            print("Warning: Progress file is corrupted")
        except Exception as e:
            print(f"Warning: Could not load progress file: {e}")

    # Try to rebuild from existing files
    return scan_existing_files(OUTPUT_DIR)


def save_progress(progress):
    """Save progress for resume"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def select_geographies():
    """Let user select which geography levels to download"""
    print("\n" + "="*60)
    print("SELECT GEOGRAPHY LEVELS")
    print("="*60)
    print("\nAvailable geography levels (most granular first):")

    for i, (for_clause, in_clause, label, desc) in enumerate(GEOGRAPHY_LEVELS, 1):
        print(f"  {i}. {label}: {desc}")

    print("\n  A. ALL levels (download everything)")
    print("  D. DEFAULT (try all, use first that works)")
    print("  C. Custom selection")

    choice = input("\nEnter choice (A/D/C or numbers like 1,2,5): ").strip().upper()

    if choice == 'A':
        return [(g[0], g[1]) for g in GEOGRAPHY_LEVELS]
    elif choice == 'D' or choice == '':
        return [(g[0], g[1]) for g in GEOGRAPHY_LEVELS]
    elif choice == 'C':
        print("\nEnter numbers separated by commas (e.g., 1,2,5):")
        nums = input("Selection: ").strip()
        try:
            indices = [int(n.strip()) - 1 for n in nums.split(',')]
            return [(GEOGRAPHY_LEVELS[i][0], GEOGRAPHY_LEVELS[i][1]) for i in indices if 0 <= i < len(GEOGRAPHY_LEVELS)]
        except KeyboardInterrupt:
            raise
        except Exception:
            print("Invalid selection, using default")
            return [(g[0], g[1]) for g in GEOGRAPHY_LEVELS]
    else:
        try:
            indices = [int(n.strip()) - 1 for n in choice.split(',')]
            return [(GEOGRAPHY_LEVELS[i][0], GEOGRAPHY_LEVELS[i][1]) for i in indices if 0 <= i < len(GEOGRAPHY_LEVELS)]
        except KeyboardInterrupt:
            raise
        except Exception:
            return [(g[0], g[1]) for g in GEOGRAPHY_LEVELS]


def get_geo_label(geography):
    """Create a filename-safe label from geography specification"""
    if isinstance(geography, tuple):
        for_clause, in_clause = geography
        label = for_clause.replace(':', '_').replace('*', 'all').replace(',', '_').replace(' ', '_')
        if in_clause:
            in_label = in_clause.replace(':', '_').replace('*', 'all').replace(',', '_').replace('&in=', '_in_').replace(' ', '_')
            label = f"{label}_in_{in_label}"
    else:
        label = geography.replace(':', '_').replace('*', 'all').replace(',', '_').replace(' ', '_')
    return label


def create_folder_structure(dataset_path, year, output_dir):
    """Create folder structure: {category}/{year}/{sub_path}/"""
    parts = dataset_path.split('/') if dataset_path else ['unknown']
    category = parts[0] if parts else 'unknown'
    sub_path = '/'.join(parts[1:]) if len(parts) > 1 else ''

    if sub_path:
        folder = os.path.join(output_dir, category, str(year), sub_path)
    else:
        folder = os.path.join(output_dir, category, str(year))

    os.makedirs(folder, exist_ok=True)
    return folder


def update_master_index(title, year, dataset_path, tables_downloaded, folder):
    """Update the master download index"""
    file_exists = os.path.exists(INDEX_FILE)

    with open(INDEX_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'title', 'year', 'dataset_path',
                            'tables_downloaded', 'folder_path'])
        writer.writerow([
            datetime.now().isoformat(),
            title,
            year,
            dataset_path,
            tables_downloaded,
            folder
        ])


def save_data_dictionary(folder, all_var_metadata):
    """Save consolidated data dictionary for a dataset folder"""
    dict_path = os.path.join(folder, "_data_dictionary.csv")

    combined = {}
    for table_name, var_meta in all_var_metadata.items():
        for var_name, meta in var_meta.items():
            if var_name not in combined:
                combined[var_name] = {
                    'variable': var_name,
                    'label': meta['label'],
                    'type': meta['type'],
                    'tables': [table_name]
                }
            else:
                if table_name not in combined[var_name]['tables']:
                    combined[var_name]['tables'].append(table_name)

    with open(dict_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['variable', 'label', 'type', 'appears_in_tables'])
        for var_name in sorted(combined.keys()):
            meta = combined[var_name]
            writer.writerow([
                var_name,
                meta['label'],
                meta['type'],
                '; '.join(meta['tables'])
            ])

    return dict_path


# =============================================================================
# API FUNCTIONS
# =============================================================================

def get_all_groups(year, dataset_path):
    """Get all table groups for a dataset"""
    year = int(float(year)) if year else year
    url = f"{API_BASE}/{year}/{dataset_path}/groups.json"

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            groups = data.get('groups', [])
            return [(g.get('name', ''), g.get('description', '')) for g in groups]
    except KeyboardInterrupt:
        raise
    except Exception:
        pass

    return []


def get_table_variables(year, dataset_path, group_name):
    """Get all variables for a specific table/group"""
    year = int(float(year)) if year else year
    url = f"{API_BASE}/{year}/{dataset_path}/groups/{group_name}.json"

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            variables = data.get('variables', {})
            var_names = [v for v in variables.keys() if not v.startswith('_')]
            var_metadata = {}
            for v in var_names:
                label = variables[v].get('label', '').replace('!!', ' | ')
                var_metadata[v] = {
                    'label': label,
                    'concept': variables[v].get('concept', ''),
                    'type': variables[v].get('predicateType', '')
                }
            return var_names, var_metadata
    except KeyboardInterrupt:
        raise
    except Exception:
        pass

    return [], {}


def download_table(year, dataset_path, variables, geography, api_key):
    """Download data for specific variables and geography"""
    year = int(float(year)) if year else year
    var_string = ','.join(variables[:MAX_VARS_PER_REQUEST])
    url = f"{API_BASE}/{year}/{dataset_path}"

    # Handle geography as tuple (for_clause, in_clause) or string
    if isinstance(geography, tuple):
        for_clause, in_clause = geography
        params = {
            'get': var_string,
            'for': for_clause,
            'key': api_key
        }
        if in_clause:
            params['in'] = in_clause
    else:
        params = {
            'get': var_string,
            'for': geography,
            'key': api_key
        }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [204, 400, 500]:
                return None
        except requests.exceptions.Timeout:
            pass
        except KeyboardInterrupt:
            raise
        except Exception:
            pass
        time.sleep(RATE_LIMIT * 2)

    return None


def save_table_csv(data, filepath):
    """Save table data to CSV"""
    if not data or len(data) < 2:
        return False

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)
    return True


def download_dataset(row, output_dir, api_key, geographies, progress, log_writer):
    """Download all tables for a grouped dataset"""
    title = row.get('title', 'Unknown')
    year = row.get('year', row.get('vintage', ''))
    dataset_path = row.get('dataset_path', '')

    year = int(float(year)) if year else year
    dataset_key = f"{dataset_path}/{year}"

    if not year or not dataset_path:
        return 0

    if dataset_key in progress['completed_datasets']:
        print(f"  Skipping (already done): {title[:40]}")
        return 0

    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"Path: {dataset_path}, Year: {year}")
    print('='*60)

    folder = create_folder_structure(dataset_path, year, output_dir)
    groups = get_all_groups(year, dataset_path)

    if not groups:
        print(f"  No tables found")
        progress['completed_datasets'].append(dataset_key)
        return 0

    print(f"  Found {len(groups)} tables")

    downloaded = 0
    all_var_metadata = {}

    for group_name, group_desc in groups:
        if not group_name:
            continue

        table_key = f"{dataset_key}/{group_name}"
        if table_key in progress['completed_tables']:
            continue

        variables, var_metadata = get_table_variables(year, dataset_path, group_name)
        if not variables:
            continue

        all_var_metadata[group_name] = var_metadata

        for geo in geographies:
            data = download_table(year, dataset_path, variables, geo, api_key)

            if data and len(data) > 1:
                geo_label = get_geo_label(geo)
                filename = f"{group_name}_{geo_label}.csv"
                filepath = os.path.join(folder, filename)

                if save_table_csv(data, filepath):
                    downloaded += 1
                    progress['total_downloaded'] += 1
                    progress['completed_tables'].append(table_key)

                    print(f"    {group_name} ({geo_label}): {len(data)-1} rows")
                    log_writer.writerow([datetime.now(), title, year, dataset_path,
                                        group_name, geo_label, len(data)-1, filepath])
                    break

            time.sleep(RATE_LIMIT)

    progress['completed_datasets'].append(dataset_key)
    save_progress(progress)

    if all_var_metadata:
        dict_path = save_data_dictionary(folder, all_var_metadata)
        print(f"  Saved data dictionary: {os.path.basename(dict_path)}")

    if downloaded > 0:
        update_master_index(title, year, dataset_path, downloaded, folder)

    print(f"  Downloaded {downloaded}/{len(groups)} tables")
    return downloaded


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main download function"""
    print("="*60)
    print("CENSUS GROUPED TABLE DOWNLOADER")
    print("For datasets with table groups (ACS, Decennial, etc.)")
    print("="*60)

    if API_KEY == "YOUR_API_KEY_HERE":
        print("\nERROR: Please set your Census API key in the script!")
        print("Get one at: https://api.census.gov/data/key_signup.html")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    progress = load_progress()

    if progress['total_downloaded'] > 0:
        print(f"\nFound previous progress:")
        print(f"  Tables downloaded: {progress['total_downloaded']}")
        print(f"  Completed datasets: {len(progress['completed_datasets'])}")
        resume = input("Resume from where you left off? (yes/no): ").strip().lower()
        if resume != 'yes':
            progress = {
                'completed_datasets': [],
                'completed_tables': [],
                'total_downloaded': 0,
            }
            print("Starting fresh...")

    print(f"\nLoading catalog: {CATALOG_FILE}")
    df = load_catalog(CATALOG_FILE)

    df = select_categories(df)
    if len(df) == 0:
        print("No datasets to download. Exiting.")
        return

    geographies = select_geographies()
    print(f"\nSelected {len(geographies)} geography level(s)")

    print(f"\n{'='*60}")
    print("DOWNLOAD SUMMARY")
    print('='*60)
    print(f"Total datasets: {len(df)}")
    print(f"Already completed: {len(progress['completed_datasets'])}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"\nPress Ctrl+C anytime to stop - progress is saved automatically.")

    confirm = input("\nStart downloading? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Aborted.")
        return

    log_mode = 'a' if progress['total_downloaded'] > 0 else 'w'
    with open(LOG_FILE, log_mode, newline='', encoding='utf-8') as log_f:
        log_writer = csv.writer(log_f)

        if log_mode == 'w':
            log_writer.writerow(['timestamp', 'dataset_title', 'year', 'dataset_path',
                                'table_name', 'geography', 'rows', 'filepath'])

        start_time = datetime.now()
        session_downloaded = 0

        for idx, row in df.iterrows():
            try:
                downloaded = download_dataset(row, OUTPUT_DIR, API_KEY, geographies, progress, log_writer)
                session_downloaded += max(0, downloaded)
                log_f.flush()

            except KeyboardInterrupt:
                print("\n\nInterrupted! Progress saved.")
                save_progress(progress)
                break
            except Exception as e:
                print(f"  ERROR: {e}")
                continue

        elapsed = datetime.now() - start_time

    print(f"\n{'='*60}")
    print("SESSION COMPLETE")
    print('='*60)
    print(f"Downloaded this session: {session_downloaded}")
    print(f"Total tables: {progress['total_downloaded']}")
    print(f"Datasets completed: {len(progress['completed_datasets'])}")
    print(f"Time elapsed: {elapsed}")
    print(f"\nRun the script again to resume where you left off.")


if __name__ == "__main__":
    main()
