"""
Census Data Catalog - Scrapes all available Census datasets via the Discovery API
Identifies datasets, their vintages, and flags potential redundancies/subsets
Also fetches table counts and variable counts for each dataset
"""

import requests
import json
import csv
import time
from datetime import datetime
from collections import defaultdict

# Census Discovery API - no key required for this endpoint
DISCOVERY_URL = "https://api.census.gov/data.json"

# Your API key (for later use when querying actual data)
API_KEY = "YOUR_API_KEY_HERE"

OUTPUT_CSV = r"D:\data_archives_2025\census_catalog.csv"
OUTPUT_JSON = r"D:\data_archives_2025\census_catalog.json"


def fetch_catalog():
    """Fetch the full dataset catalog from Census API"""
    print(f"Fetching catalog from: {DISCOVERY_URL}")

    response = requests.get(DISCOVERY_URL, timeout=60)
    response.raise_for_status()

    data = response.json()
    datasets = data.get('dataset', [])

    print(f"Found {len(datasets)} datasets\n")
    return datasets


def get_table_count(dataset_path, vintage):
    """Fetch the number of tables/groups available for a dataset"""
    if not dataset_path or not vintage:
        return None, None

    # Build the groups URL
    base_url = f"https://api.census.gov/data/{vintage}/{dataset_path}/groups.json"

    try:
        response = requests.get(base_url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            groups = data.get('groups', [])
            return len(groups), None
    except:
        pass

    return None, None


def get_variable_count(dataset_path, vintage):
    """Fetch the number of variables available for a dataset"""
    if not dataset_path or not vintage:
        return None

    # Build the variables URL
    base_url = f"https://api.census.gov/data/{vintage}/{dataset_path}/variables.json"

    try:
        response = requests.get(base_url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            variables = data.get('variables', {})
            return len(variables)
    except:
        pass

    return None


def estimate_size_mb(variable_count, table_count):
    """Rough estimate of data size based on variables and tables"""
    if variable_count is None:
        return None
    # Very rough estimate: ~1KB per variable for full geographic coverage
    # This is a ballpark - actual size varies widely
    estimated_kb = variable_count * 1
    return round(estimated_kb / 1024, 2)


def parse_dataset(ds):
    """Extract key info from a dataset entry"""
    return {
        'title': ds.get('title', ''),
        'identifier': ds.get('identifier', ''),
        'description': ds.get('description', '')[:500] if ds.get('description') else '',
        'year': ds.get('c_vintage', ''),
        'vintage': ds.get('c_vintage', ''),
        'temporal': ds.get('temporal', ''),
        'dataset_path': '/'.join(ds.get('c_dataset', [])),
        'modified': ds.get('modified', ''),
        'is_microdata': ds.get('c_isMicrodata', False),
        'is_aggregate': ds.get('c_isAggregate', False),
        'is_cube': ds.get('c_isCube', False),
        'is_available': ds.get('c_isAvailable', False),
        'access_url': '',
        'variables_url': '',
        'geographies_url': '',
        'table_count': None,
        'variable_count': None,
        'estimated_size_mb': None,
        'keywords': ', '.join(ds.get('keyword', [])),
        'contact': ds.get('contactPoint', {}).get('fn', ''),
        'publisher': ds.get('publisher', {}).get('name', ''),
    }


def extract_urls(ds, parsed):
    """Extract API URLs from distribution and references"""
    # Get API endpoint from distribution
    for dist in ds.get('distribution', []):
        if dist.get('@type') == 'dcat:Distribution':
            parsed['access_url'] = dist.get('accessURL', '')
            break

    # Get variables and geography URLs from references
    for ref in ds.get('c_documentationLink', []) or []:
        if 'variables' in str(ref).lower():
            parsed['variables_url'] = ref
        elif 'geography' in str(ref).lower():
            parsed['geographies_url'] = ref

    return parsed


def fetch_dataset_details(parsed, index, total):
    """Fetch table and variable counts for a dataset"""
    dataset_path = parsed['dataset_path']
    vintage = parsed['vintage']

    if not parsed['is_available']:
        return parsed

    print(f"  [{index}/{total}] {parsed['title'][:50]}...", end=' ')

    # Get table count
    table_count, _ = get_table_count(dataset_path, vintage)
    parsed['table_count'] = table_count

    # Get variable count
    variable_count = get_variable_count(dataset_path, vintage)
    parsed['variable_count'] = variable_count

    # Estimate size
    parsed['estimated_size_mb'] = estimate_size_mb(variable_count, table_count)

    print(f"tables={table_count}, vars={variable_count}")

    # Rate limiting
    time.sleep(0.2)

    return parsed


def find_redundancies(datasets):
    """
    Identify potential redundancies:
    - Same dataset with different vintages
    - Datasets that appear to be subsets of others
    """
    # Group by base name (without vintage year)
    groups = defaultdict(list)

    for ds in datasets:
        # Create a base identifier by removing year patterns
        title = ds['title']
        base_name = title

        # Remove common year patterns
        for year in range(2000, 2030):
            base_name = base_name.replace(str(year), '').strip()

        base_name = ' '.join(base_name.split())  # Clean up whitespace
        groups[base_name].append(ds)

    # Find groups with multiple vintages
    multi_vintage = {k: v for k, v in groups.items() if len(v) > 1}

    return multi_vintage


def main():
    # Fetch all datasets
    datasets = fetch_catalog()

    # Parse each dataset
    parsed_datasets = []
    for ds in datasets:
        parsed = parse_dataset(ds)
        parsed = extract_urls(ds, parsed)
        parsed_datasets.append(parsed)

    # Sort by dataset path and vintage
    parsed_datasets.sort(key=lambda x: (x['dataset_path'], x['vintage']))

    # Fetch table and variable counts for each available dataset
    print("\nFetching table and variable counts...")
    print("(This may take a few minutes)\n")

    available_count = len([d for d in parsed_datasets if d['is_available']])
    idx = 0
    for parsed in parsed_datasets:
        if parsed['is_available']:
            idx += 1
            fetch_dataset_details(parsed, idx, available_count)

    # Find potential redundancies
    redundancies = find_redundancies(parsed_datasets)

    # Save to CSV
    print(f"\nSaving to CSV: {OUTPUT_CSV}")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'title', 'year', 'vintage', 'temporal', 'dataset_path', 'is_available',
            'table_count', 'variable_count', 'estimated_size_mb',
            'is_microdata', 'is_aggregate', 'is_cube',
            'access_url', 'variables_url', 'geographies_url',
            'keywords', 'publisher', 'contact', 'modified',
            'description', 'identifier'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(parsed_datasets)

    # Save full JSON for reference
    print(f"Saving to JSON: {OUTPUT_JSON}")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(parsed_datasets, f, indent=2)

    # Print summary
    print(f"\n{'='*60}")
    print("CENSUS DATA CATALOG SUMMARY")
    print('='*60)
    print(f"Total datasets: {len(parsed_datasets)}")
    print(f"Available datasets: {len([d for d in parsed_datasets if d['is_available']])}")
    print(f"Microdata datasets: {len([d for d in parsed_datasets if d['is_microdata']])}")
    print(f"Aggregate datasets: {len([d for d in parsed_datasets if d['is_aggregate']])}")

    # Table and variable totals
    total_tables = sum(d['table_count'] or 0 for d in parsed_datasets)
    total_vars = sum(d['variable_count'] or 0 for d in parsed_datasets)
    total_size = sum(d['estimated_size_mb'] or 0 for d in parsed_datasets)
    print(f"\nTotal tables across all datasets: {total_tables:,}")
    print(f"Total variables across all datasets: {total_vars:,}")
    print(f"Estimated total size: {total_size:,.2f} MB")

    # List unique dataset types
    dataset_types = set(d['dataset_path'].split('/')[0] for d in parsed_datasets if d['dataset_path'])
    print(f"\nUnique dataset categories ({len(dataset_types)}):")
    for dt in sorted(dataset_types):
        count = len([d for d in parsed_datasets if d['dataset_path'].startswith(dt)])
        print(f"  - {dt}: {count} datasets")

    # Show largest datasets
    print(f"\n{'='*60}")
    print("LARGEST DATASETS (by variable count)")
    print('='*60)
    by_vars = sorted([d for d in parsed_datasets if d['variable_count']],
                     key=lambda x: x['variable_count'] or 0, reverse=True)[:10]
    for d in by_vars:
        print(f"  {d['variable_count']:,} vars - {d['title'][:60]}")

    print(f"\n{'='*60}")
    print("LARGEST DATASETS (by table count)")
    print('='*60)
    by_tables = sorted([d for d in parsed_datasets if d['table_count']],
                       key=lambda x: x['table_count'] or 0, reverse=True)[:10]
    for d in by_tables:
        print(f"  {d['table_count']:,} tables - {d['title'][:60]}")

    # Show redundancies
    print(f"\n{'='*60}")
    print("POTENTIAL REDUNDANCIES (same dataset, multiple vintages)")
    print('='*60)
    for base_name, group in sorted(redundancies.items(), key=lambda x: -len(x[1]))[:20]:
        vintages = sorted(set(d['vintage'] for d in group if d['vintage']))
        if len(vintages) > 1:
            print(f"\n{base_name}")
            print(f"  Vintages: {', '.join(str(v) for v in vintages)}")

    print(f"\n\nDone! Files saved to:")
    print(f"  - {OUTPUT_CSV}")
    print(f"  - {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
