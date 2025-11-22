import requests
import json
import csv
from pathlib import Path
import time

def get_all_datasets(api_key, base_url):
    """Get list of all available BEA datasets"""
    print("Fetching all available BEA datasets...")

    params = {
        "UserID": api_key,
        "method": "GetDataSetList",
        "ResultFormat": "JSON"
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'Dataset' in results:
                return results['Dataset']
    except Exception as e:
        print(f"Error fetching datasets: {str(e)}")

    return []


def get_tables_for_dataset(api_key, base_url, dataset_name):
    """Get all tables for a specific dataset"""
    print(f"  Fetching tables for {dataset_name}...")

    params = {
        "UserID": api_key,
        "method": "GetParameterValues",
        "datasetname": dataset_name,
        "ParameterName": "TableName",
        "ResultFormat": "JSON"
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'BEAAPI' in data:
            # Check for errors
            if 'Error' in data['BEAAPI']:
                error_detail = data['BEAAPI']['Error'].get('APIErrorDescription', 'Unknown error')
                print(f"    Note: {error_detail}")
                return []

            # Get results
            if 'Results' in data['BEAAPI']:
                results = data['BEAAPI']['Results']
                if 'ParamValue' in results:
                    # Normalize the data - API sometimes uses different field names
                    tables = results['ParamValue']
                    normalized = []
                    for table in tables:
                        if isinstance(table, dict):
                            # Different APIs use different field names
                            code = table.get('Key') or table.get('TableName') or table.get('TableID') or 'N/A'
                            desc = table.get('Desc') or table.get('Description') or table.get('TableDescription') or 'N/A'
                            normalized.append({
                                'Key': code,
                                'Desc': desc
                            })
                    return normalized
    except Exception as e:
        print(f"    Error: {str(e)}")

    return []


def get_parameters_for_dataset(api_key, base_url, dataset_name):
    """Get all parameters available for a dataset"""
    params = {
        "UserID": api_key,
        "method": "GetParameterList",
        "datasetname": dataset_name,
        "ResultFormat": "JSON"
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'BEAAPI' in data:
            # Check for errors
            if 'Error' in data['BEAAPI']:
                return []

            if 'Results' in data['BEAAPI']:
                results = data['BEAAPI']['Results']
                if 'Parameter' in results:
                    params_list = results['Parameter']
                    # Ensure it's a list
                    if isinstance(params_list, list):
                        return params_list
    except Exception as e:
        print(f"    Error getting parameters: {str(e)}")

    return []


def enumerate_all_bea_tables(api_key, output_dir="bea_table_inventory"):
    """
    Comprehensive enumeration of ALL tables available in the BEA API
    across ALL datasets
    """
    base_url = "https://apps.bea.gov/api/data"

    print("="*80)
    print("BEA API COMPREHENSIVE TABLE ENUMERATION")
    print("="*80)
    print()

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Step 1: Get all datasets
    datasets = get_all_datasets(api_key, base_url)

    if not datasets:
        print("✗ Failed to retrieve datasets. Check your API key.")
        return

    print(f"\n✓ Found {len(datasets)} datasets")
    print("-"*80)
    for ds in datasets:
        print(f"  - {ds['DatasetName']}: {ds['DatasetDescription']}")
    print()

    # Step 2: For each dataset, get all tables and parameters
    all_tables = []
    dataset_details = []

    for dataset in datasets:
        dataset_name = dataset['DatasetName']
        dataset_desc = dataset['DatasetDescription']

        print(f"\n{'='*80}")
        print(f"Processing: {dataset_name}")
        print(f"Description: {dataset_desc}")
        print('='*80)

        # Get parameters for this dataset
        parameters = get_parameters_for_dataset(api_key, base_url, dataset_name)

        # Check if TableName is a parameter (only if parameters is a valid list)
        has_table_param = False
        if parameters and isinstance(parameters, list):
            has_table_param = any(p.get('ParameterName') == 'TableName' for p in parameters if isinstance(p, dict))

        if has_table_param:
            # Get all tables
            tables = get_tables_for_dataset(api_key, base_url, dataset_name)

            if tables:
                print(f"  ✓ Found {len(tables)} tables")

                for table in tables:
                    table_code = table.get('Key', 'N/A')
                    table_desc = table.get('Desc', 'N/A')

                    all_tables.append({
                        'Dataset': dataset_name,
                        'Dataset_Description': dataset_desc,
                        'Table_Code': table_code,
                        'Table_Description': table_desc
                    })

                    print(f"    - {table_code}: {table_desc}")
            else:
                print(f"  ✗ No tables found or error retrieving tables")
        else:
            print(f"  Note: This dataset does not use 'TableName' parameter")
            if parameters:
                print(f"  Available parameters:")
                for param in parameters:
                    if isinstance(param, dict):
                        param_name = param.get('ParameterName', 'N/A')
                        param_desc = param.get('ParameterDescription', 'N/A')
                        print(f"    - {param_name}: {param_desc}")
            else:
                print(f"  No parameters retrieved")

        # Store dataset details
        param_names = ', '.join([p.get('ParameterName', 'N/A') for p in parameters if isinstance(p, dict)]) if parameters else 'None'
        dataset_details.append({
            'Dataset_Name': dataset_name,
            'Dataset_Description': dataset_desc,
            'Has_Tables': 'Yes' if has_table_param else 'No',
            'Table_Count': len(get_tables_for_dataset(api_key, base_url, dataset_name)) if has_table_param else 0,
            'Parameters': param_names
        })

        # Be nice to the API
        time.sleep(0.5)

    # Step 3: Save comprehensive results
    print(f"\n{'='*80}")
    print("SAVING RESULTS")
    print('='*80)

    # Save all tables to CSV
    if all_tables:
        csv_path = Path(output_dir) / "BEA_All_Tables_Comprehensive.csv"
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['Dataset', 'Dataset_Description', 'Table_Code', 'Table_Description'])
            writer.writeheader()
            writer.writerows(all_tables)
        print(f"✓ Saved {len(all_tables)} tables to: {csv_path}")

    # Save dataset summary
    csv_path = Path(output_dir) / "BEA_Datasets_Summary.csv"
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['Dataset_Name', 'Dataset_Description', 'Has_Tables', 'Table_Count', 'Parameters'])
        writer.writeheader()
        writer.writerows(dataset_details)
    print(f"✓ Saved {len(dataset_details)} dataset summaries to: {csv_path}")

    # Save JSON for detailed inspection
    json_path = Path(output_dir) / "BEA_Complete_Inventory.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            'datasets': dataset_details,
            'all_tables': all_tables,
            'total_datasets': len(datasets),
            'total_tables': len(all_tables)
        }, f, indent=2)
    print(f"✓ Saved complete inventory to: {json_path}")

    # Print summary statistics
    print(f"\n{'='*80}")
    print("SUMMARY STATISTICS")
    print('='*80)
    print(f"Total Datasets: {len(datasets)}")
    print(f"Total Tables Found: {len(all_tables)}")
    print()

    # Tables by dataset
    from collections import Counter
    tables_by_dataset = Counter([t['Dataset'] for t in all_tables])
    print("Tables by Dataset:")
    for dataset, count in sorted(tables_by_dataset.items(), key=lambda x: x[1], reverse=True):
        print(f"  {dataset:30} - {count:4} tables")

    print(f"\n{'='*80}")
    print(f"All results saved to: {Path(output_dir).absolute()}")
    print('='*80)

    return all_tables, dataset_details


if __name__ == "__main__":
    print("="*80)
    print("BEA API - Complete Table Enumeration Tool")
    print("="*80)
    print()
    print("This script will enumerate ALL tables across ALL BEA datasets.")
    print("You need a free BEA API key from: https://apps.bea.gov/API/signup/")
    print()

    api_key = input("Enter your BEA API key: ").strip()

    if not api_key:
        print("\n✗ Error: API key is required!")
        print("Register at: https://apps.bea.gov/API/signup/")
    else:
        # Allow custom output directory
        custom_dir = input("\nOutput directory (press Enter for 'bea_table_inventory'): ").strip()
        output_dir = custom_dir if custom_dir else "bea_table_inventory"

        enumerate_all_bea_tables(api_key, output_dir)
