import requests
import csv
import time
import os
from pathlib import Path

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


def get_line_codes_for_table(api_key, base_url, table_name):
    """Get all line codes for a specific table"""
    params = {
        "UserID": api_key,
        "method": "GetParameterValuesFiltered",
        "datasetname": "Regional",
        "TargetParameter": "LineCode",
        "TableName": table_name,
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
                    desc = item['Desc'].replace(f'[{table_name}] ', '')
                    clean_desc = desc.replace('/', '_').replace('(', '').replace(')', '').replace(',', '').replace(' ', '_').replace(':', '')
                    # Limit filename length
                    if len(clean_desc) > 80:
                        clean_desc = clean_desc[:80]
                    line_codes[key] = clean_desc
                return line_codes
    except Exception as e:
        print(f"    ✗ Error fetching line codes: {str(e)}")
    
    return {}


def download_cainc_all_statistics(output_dir="bea_cainc_all_statistics"):
    """
    Download CAINC tables with ALL statistics types
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"✓ Created output directory: {os.path.abspath(output_dir)}\n")
    
    base_url = "https://apps.bea.gov/api/data"
    
    print("=" * 70)
    print("BEA CAINC Complete Statistics Downloader")
    print("=" * 70)
    print("\nThis will download CAINC1, CAINC4, CAINC30, and other CAINC tables")
    print("to capture all available statistics including levels, percent changes, etc.")
    print()
    
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
    
    # CAINC tables - these contain different statistics/views of the data
    cainc_tables = {
        "CAINC1": "Personal_Income_Summary",
        "CAINC4": "Personal_Income_By_Major_Component",
        "CAINC30": "Economic_Profile",
        "CAINC5N": "Personal_Income_By_Component_and_NAICS",
        "CAINC6N": "Compensation_By_NAICS_Industry",
        "CAINC91": "Gross_Flow_of_Earnings"
    }
    
    # Geographic types
    geo_types = {
        "STATE": "State",
        "MSA": "Metropolitan_Statistical_Area",
        "MIC": "Micropolitan_Statistical_Area",
        "CSA": "Combined_Statistical_Area",
        "MET": "Metropolitan_Division",
        "PORT": "Metropolitan_Nonmetropolitan_Portions"
    }
    
    print("Discovering all line codes for each CAINC table...\n")
    
    # Get line codes for each table
    table_line_codes = {}
    for table_name, table_desc in cainc_tables.items():
        print(f"  Fetching line codes for {table_name}...")
        codes = get_line_codes_for_table(api_key, base_url, table_name)
        if codes:
            table_line_codes[table_name] = codes
            print(f"    ✓ Found {len(codes)} line codes")
        else:
            print(f"    ⚠ No line codes found")
    
    # Calculate total
    total_tables = sum(
        len(geo_types) * len(codes) 
        for codes in table_line_codes.values()
    )
    
    print(f"\n{'='*70}")
    print(f"Will download {total_tables} total files")
    print(f"{'='*70}\n")
    
    success_count = 0
    current = 0
    
    for table_name, table_desc in cainc_tables.items():
        if table_name not in table_line_codes:
            continue
            
        line_codes = table_line_codes[table_name]
        
        print(f"\n{'='*70}")
        print(f"TABLE: {table_name} - {table_desc}")
        print(f"{'='*70}")
        
        for geo_code, geo_name in geo_types.items():
            print(f"\n  Geographic Type: {geo_name} ({geo_code})")
            
            # Create directory structure: output_dir/TableName/GeoType/
            table_geo_dir = f"{output_dir}/{table_name}/{geo_name}"
            Path(table_geo_dir).mkdir(parents=True, exist_ok=True)
            
            for line_code, line_desc in line_codes.items():
                current += 1
                params = {
                    "UserID": api_key,
                    "method": "GetData",
                    "datasetname": "Regional",
                    "TableName": table_name,
                    "LineCode": line_code,
                    "GeoFips": geo_code,
                    "Year": "ALL",
                    "ResultFormat": "JSON"
                }
                
                try:
                    print(f"    [{current}/{total_tables}] Line {line_code}: {line_desc[:40]}...", end=" ")
                    response = requests.get(base_url, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    if 'BEAAPI' in data:
                        if 'Error' in data['BEAAPI']:
                            print(f"✗ {data['BEAAPI']['Error']['APIErrorDescription']}")
                            continue
                            
                        if 'Results' in data['BEAAPI']:
                            results = data['BEAAPI']['Results']
                            if 'Data' in results and results['Data']:
                                csv_filename = f"{table_geo_dir}/{table_name}_{geo_code}_Line{line_code}_{line_desc}.csv"
                                
                                if save_as_csv(results['Data'], csv_filename):
                                    row_count = len(results['Data'])
                                    print(f"✓ {row_count} rows")
                                    success_count += 1
                                else:
                                    print(f"✗ Failed to save")
                            else:
                                print(f"⚠ No data")
                        else:
                            print(f"✗ No Results")
                    else:
                        print(f"✗ Bad format")
                    
                    time.sleep(0.5)
                    
                except requests.exceptions.RequestException as e:
                    print(f"✗ Network error")
                except Exception as e:
                    print(f"✗ Error: {str(e)}")
    
    print("\n" + "=" * 70)
    print(f"Download complete! {success_count}/{total_tables} files saved.")
    print(f"Location: {os.path.abspath(output_dir)}")
    print("=" * 70)
    
    # Print summary
    print("\nSummary by Table and Geography:")
    for table_name in cainc_tables.keys():
        table_dir = f"{output_dir}/{table_name}"
        if os.path.exists(table_dir):
            print(f"\n  {table_name}:")
            for geo_code, geo_name in geo_types.items():
                geo_dir = f"{table_dir}/{geo_name}"
                if os.path.exists(geo_dir):
                    file_count = len([f for f in os.listdir(geo_dir) if f.endswith('.csv')])
                    if file_count > 0:
                        print(f"    {geo_name}: {file_count} files")


if __name__ == "__main__":
    download_cainc_all_statistics()
