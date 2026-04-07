import requests
import csv
import time
import os
from pathlib import Path
import json

def save_as_csv(data, filename):
    """Convert JSON data to CSV format"""
    if not data:
        return False
    
    try:
        all_headers = set()
        for row in data:
            all_headers.update(row.keys())
        
        headers = sorted(list(all_headers))
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
        
        return True
    except Exception as e:
        print(f"    Error saving CSV: {str(e)}")
        return False


def get_all_datasets(api_key, base_url):
    """Get list of all available datasets"""
    params = {
        "UserID": api_key,
        "method": "GetDataSetList",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
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
    
    # For NIPA and NIUnderlyingDetail, get table list
    if dataset_name in ['NIPA', 'NIUnderlyingDetail']:
        params = {
            "UserID": api_key,
            "method": "GetParameterValues",
            "datasetname": dataset_name,
            "ParameterName": "TableName",
            "ResultFormat": "JSON"
        }
    elif dataset_name == 'FixedAssets':
        params = {
            "UserID": api_key,
            "method": "GetParameterValues",
            "datasetname": dataset_name,
            "ParameterName": "TableName",
            "ResultFormat": "JSON"
        }
    else:
        return []
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'ParamValue' in results:
                return results['ParamValue']
    except Exception as e:
        print(f"    Error fetching tables: {str(e)}")
    
    return []


def download_nipa_table(api_key, base_url, table_name, output_dir):
    """Download a NIPA table with all frequencies and years"""
    
    frequencies = ['A', 'Q', 'M']  # Annual, Quarterly, Monthly
    success = False
    
    for freq in frequencies:
        params = {
            "UserID": api_key,
            "method": "GetData",
            "DataSetName": "NIPA",
            "TableName": table_name,
            "Frequency": freq,
            "Year": "ALL",
            "ResultFormat": "JSON"
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if 'BEAAPI' in data:
                if 'Error' in data['BEAAPI']:
                    continue  # This frequency not available for this table
                    
                if 'Results' in data['BEAAPI']:
                    results = data['BEAAPI']['Results']
                    if 'Data' in results and results['Data']:
                        filename = f"{output_dir}/NIPA_{table_name}_{freq}.csv"
                        if save_as_csv(results['Data'], filename):
                            print(f"      {freq}: ✓ {len(results['Data'])} rows")
                            success = True
                        time.sleep(0.5)
        except Exception as e:
            continue
    
    return success


def download_ni_underlying_table(api_key, base_url, table_name, output_dir):
    """Download a NIPA Underlying Detail table"""
    
    frequencies = ['A', 'Q', 'M']
    success = False
    
    for freq in frequencies:
        params = {
            "UserID": api_key,
            "method": "GetData",
            "DataSetName": "NIUnderlyingDetail",
            "TableName": table_name,
            "Frequency": freq,
            "Year": "ALL",
            "ResultFormat": "JSON"
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if 'BEAAPI' in data:
                if 'Error' in data['BEAAPI']:
                    continue
                    
                if 'Results' in data['BEAAPI']:
                    results = data['BEAAPI']['Results']
                    if 'Data' in results and results['Data']:
                        filename = f"{output_dir}/NIUnderlyingDetail_{table_name}_{freq}.csv"
                        if save_as_csv(results['Data'], filename):
                            print(f"      {freq}: ✓ {len(results['Data'])} rows")
                            success = True
                        time.sleep(0.5)
        except Exception as e:
            continue
    
    return success


def download_fixed_assets_table(api_key, base_url, table_name, output_dir):
    """Download a Fixed Assets table"""
    
    params = {
        "UserID": api_key,
        "method": "GetData",
        "DataSetName": "FixedAssets",
        "TableName": table_name,
        "Year": "ALL",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data:
            if 'Error' in data['BEAAPI']:
                return False
                
            if 'Results' in data['BEAAPI']:
                results = data['BEAAPI']['Results']
                if 'Data' in results and results['Data']:
                    filename = f"{output_dir}/FixedAssets_{table_name}.csv"
                    if save_as_csv(results['Data'], filename):
                        print(f"      ✓ {len(results['Data'])} rows")
                        time.sleep(0.5)
                        return True
    except Exception as e:
        pass
    
    return False


def download_national_accounts(output_dir=None):
    """
    Download ALL National Economic Accounts data from BEA
    """
    base_url = "https://apps.bea.gov/api/data"
    
    print("=" * 70)
    print("BEA NATIONAL ECONOMIC ACCOUNTS COMPLETE DOWNLOADER")
    print("=" * 70)
    print("\nThis will download:")
    print("  • NIPA (National Income and Product Accounts)")
    print("  • NIPA Underlying Detail")
    print("  • Fixed Assets")
    print("  • All frequencies (Annual, Quarterly, Monthly)")
    print("  • All years available")
    print()
    
    # Ask for output directory
    if output_dir is None:
        print("Where would you like to save the files?")
        print("Examples:")
        print("  Current directory: bea_national_accounts")
        print("  External drive (Windows): E:\\bea_data")
        print("  External drive (Mac): /Volumes/MyDrive/bea_data")
        print("  External drive (Linux): /media/username/MyDrive/bea_data")
        print()
        output_dir = input("Enter the full path (or press Enter for current directory): ").strip()
        
        if not output_dir:
            output_dir = "bea_national_accounts"
    
    # Create output directory
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        print(f"\n✓ Output directory: {os.path.abspath(output_dir)}")
        
        # Test write access
        test_file = os.path.join(output_dir, ".test_write")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print("✓ Write access confirmed\n")
        
    except Exception as e:
        print(f"\n✗ Error: Cannot write to {output_dir}")
        print(f"   {str(e)}")
        print("\nPlease check:")
        print("  • The path exists and is accessible")
        print("  • You have write permissions")
        print("  • External drive is properly mounted")
        return
    
    api_key = input("Enter your BEA API key: ").strip()
    
    if not api_key:
        print("\n✗ Error: API key is required!")
        return
    
    # Test API key
    print(f"\nTesting API key...")
    test_params = {
        "UserID": api_key,
        "method": "GetDataSetList",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=test_params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Error' in data['BEAAPI']:
            print(f"✗ API Error: {data['BEAAPI']['Error']['ErrorDetail']}")
            return
        
        print("✓ API key is valid!\n")
    except Exception as e:
        print(f"✗ Error testing API key: {str(e)}")
        return
    
    # National Account datasets to download
    national_datasets = {
        'NIPA': 'National_Income_and_Product_Accounts',
        'NIUnderlyingDetail': 'NIPA_Underlying_Detail',
        'FixedAssets': 'Fixed_Assets'
    }
    
    total_success = 0
    total_attempted = 0
    
    for dataset_name, dataset_desc in national_datasets.items():
        print(f"\n{'='*70}")
        print(f"DATASET: {dataset_name} - {dataset_desc}")
        print(f"{'='*70}")
        
        # Create subdirectory for this dataset
        dataset_dir = f"{output_dir}/{dataset_name}"
        Path(dataset_dir).mkdir(parents=True, exist_ok=True)
        
        print(f"\n  Fetching table list...")
        tables = get_tables_for_dataset(api_key, base_url, dataset_name)
        
        if not tables:
            print(f"    ⚠ No tables found")
            continue
        
        print(f"  ✓ Found {len(tables)} tables\n")
        
        for idx, table in enumerate(tables, 1):
            # Handle different field names (Key/TableName)
            table_name = table.get('Key') or table.get('TableName') or table.get('key')
            table_desc = table.get('Desc') or table.get('Description') or table.get('desc') or 'No description'
            
            if not table_name:
                print(f"  ⚠ Skipping table with unknown structure: {table}")
                continue
            
            # Show progress
            print(f"  [{idx}/{len(tables)}] {table_name}: {table_desc[:50]}...")
            
            total_attempted += 1
            
            # Download based on dataset type
            if dataset_name == 'NIPA':
                if download_nipa_table(api_key, base_url, table_name, dataset_dir):
                    total_success += 1
            elif dataset_name == 'NIUnderlyingDetail':
                if download_ni_underlying_table(api_key, base_url, table_name, dataset_dir):
                    total_success += 1
            elif dataset_name == 'FixedAssets':
                if download_fixed_assets_table(api_key, base_url, table_name, dataset_dir):
                    total_success += 1
    
    print("\n" + "=" * 70)
    print(f"Download complete!")
    print(f"Successfully downloaded: {total_success} tables")
    print(f"Location: {os.path.abspath(output_dir)}")
    print("=" * 70)
    
    # Print summary
    print("\nSummary by Dataset:")
    for dataset_name, dataset_desc in national_datasets.items():
        dataset_dir = f"{output_dir}/{dataset_name}"
        if os.path.exists(dataset_dir):
            file_count = len([f for f in os.listdir(dataset_dir) if f.endswith('.csv')])
            if file_count > 0:
                print(f"  {dataset_name}: {file_count} files")


if __name__ == "__main__":
    download_national_accounts()
