import requests
import csv
import time
import os
from pathlib import Path

def save_as_csv(data, filename):
    """Convert JSON data to CSV format"""
    if not data:
        print(f"    Warning: No data to save")
        return False
    
    try:
        # Get all possible headers from all rows
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


def get_all_line_codes(api_key, base_url):
    """Get all available line codes for CAINC1 table"""
    print("Fetching all available CAINC1 line codes...")
    
    params = {
        "UserID": api_key,
        "method": "GetParameterValuesFiltered",
        "datasetname": "Regional",
        "TargetParameter": "LineCode",
        "TableName": "CAINC1",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'ParamValue' in results:
                line_codes = {}
                for item in results['ParamValue']:
                    key = item['Key']
                    desc = item['Desc'].replace('[CAINC1] ', '')
                    # Clean description for filename
                    clean_desc = desc.replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace(' ', '_')
                    line_codes[key] = clean_desc
                
                print(f"✓ Found {len(line_codes)} line codes\n")
                return line_codes
    except Exception as e:
        print(f"✗ Error fetching line codes: {str(e)}")
    
    return None


def download_cainc1_all_tables(output_dir="bea_cainc1_complete"):
    """
    Download ALL CAINC1 tables for all geographic types and all statistics
    """
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"✓ Created output directory: {os.path.abspath(output_dir)}\n")
    
    # BEA API endpoint
    base_url = "https://apps.bea.gov/api/data"
    
    print("=" * 70)
    print("BEA CAINC1 Complete Downloader - All Geographic Types & Statistics")
    print("=" * 70)
    print("\nThis script requires a BEA API key.")
    print("Get one free at: https://apps.bea.gov/API/signup/")
    print()
    
    # Get API key
    api_key = input("Enter your BEA API key: ").strip()
    
    if not api_key:
        print("\n✗ Error: API key is required!")
        return
    
    # Test API key
    print(f"\nTesting API key...")
    test_params = {
        "UserID": api_key,
        "method": "GetParameterValues",
        "datasetname": "Regional",
        "ParameterName": "TableName",
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
    
    # Get all available line codes for CAINC1
    line_codes = get_all_line_codes(api_key, base_url)
    
    if not line_codes:
        print("✗ Could not fetch line codes. Exiting.")
        return
    
    # Geographic types
    geo_types = {
        "STATE": "State",
        "MSA": "Metropolitan_Statistical_Area",
        "MIC": "Micropolitan_Statistical_Area",
        "CSA": "Combined_Statistical_Area",
        "MET": "Metropolitan_Division",
        "PORT": "Metropolitan_Nonmetropolitan_Portions"
    }
    
    total_tables = len(geo_types) * len(line_codes)
    print(f"Downloading {total_tables} tables ({len(geo_types)} geo types × {len(line_codes)} statistics)...\n")
    
    success_count = 0
    current = 0
    
    for geo_code, geo_name in geo_types.items():
        print(f"\n{'='*70}")
        print(f"Geographic Type: {geo_name} ({geo_code})")
        print(f"{'='*70}")
        
        # Create subdirectory for each geo type
        geo_dir = f"{output_dir}/{geo_name}"
        Path(geo_dir).mkdir(parents=True, exist_ok=True)
        
        for line_code, line_desc in line_codes.items():
            current += 1
            params = {
                "UserID": api_key,
                "method": "GetData",
                "datasetname": "Regional",
                "TableName": "CAINC1",
                "LineCode": line_code,
                "GeoFips": geo_code,
                "Year": "ALL",
                "ResultFormat": "JSON"
            }
            
            try:
                print(f"[{current}/{total_tables}] Line {line_code}: {line_desc[:50]}...")
                response = requests.get(base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'BEAAPI' in data:
                    if 'Error' in data['BEAAPI']:
                        print(f"    ✗ API Error: {data['BEAAPI']['Error']['APIErrorDescription']}")
                        continue
                        
                    if 'Results' in data['BEAAPI']:
                        results = data['BEAAPI']['Results']
                        if 'Data' in results and results['Data']:
                            csv_filename = f"{geo_dir}/CAINC1_{geo_code}_Line{line_code}_{line_desc}.csv"
                            
                            if save_as_csv(results['Data'], csv_filename):
                                row_count = len(results['Data'])
                                print(f"    ✓ Saved {row_count} rows")
                                success_count += 1
                            else:
                                print(f"    ✗ Failed to save CSV")
                        else:
                            print(f"    ⚠ No data available")
                    else:
                        print(f"    ✗ No 'Results' field in response")
                else:
                    print(f"    ✗ Unexpected response format")
                
                time.sleep(0.5)  # Rate limiting
                
            except requests.exceptions.RequestException as e:
                print(f"    ✗ Network error: {str(e)}")
            except Exception as e:
                print(f"    ✗ Unexpected error: {str(e)}")
    
    print("\n" + "=" * 70)
    print(f"Download complete! {success_count}/{total_tables} tables saved.")
    print(f"Files organized by geography in: {os.path.abspath(output_dir)}")
    print("=" * 70)
    
    # Print summary by geo type
    print("\nSummary by Geography Type:")
    for geo_code, geo_name in geo_types.items():
        geo_dir = f"{output_dir}/{geo_name}"
        if os.path.exists(geo_dir):
            file_count = len([f for f in os.listdir(geo_dir) if f.endswith('.csv')])
            print(f"  {geo_name}: {file_count} files")


if __name__ == "__main__":
    download_cainc1_all_tables()