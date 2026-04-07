import requests
import json
import time
import os
from pathlib import Path

def download_cainc1_tables(output_dir="bea_cainc1_tables"):
    """
    Download CAINC1 tables from BEA website
    """
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # BEA API endpoint for interactive tables
    base_url = "https://apps.bea.gov/api/data"
    
    # Parameters for CAINC1 table request
    params = {
        "UserID": "YOUR_API_KEY",  # Register at https://apps.bea.gov/API/signup/
        "method": "GetData",
        "datasetname": "Regional",
        "TableName": "CAINC1",
        "LineCode": "1",  # Will iterate through line codes
        "GeoFips": "STATE",  # All states
        "Year": "ALL",
        "ResultFormat": "JSON"
    }
    
    print("Note: This script uses the BEA API which requires a free API key.")
    print("Register at: https://apps.bea.gov/API/signup/")
    print()
    
    # Check if user has API key
    api_key = input("Enter your BEA API key (or press Enter to skip): ").strip()
    
    if not api_key:
        print("\nWithout an API key, I'll create an alternative scraping approach...")
        download_via_web_interface(output_dir)
        return
    
    params["UserID"] = api_key
    
    # CAINC1 line codes (major categories)
    line_codes = {
        "1": "Personal_Income",
        "2": "Population",
        "3": "Per_Capita_Personal_Income",
        "10": "Earnings_by_Place_of_Work",
        "20": "Wages_and_Salaries",
        "35": "Supplements_to_Wages",
        "40": "Employer_Contributions_Pension",
        "50": "Employer_Contributions_Insurance",
        "60": "Proprietors_Income",
        "70": "Farm_Proprietors_Income",
        "80": "Nonfarm_Proprietors_Income"
    }
    
    print(f"\nDownloading {len(line_codes)} CAINC1 tables...\n")
    
    for line_code, description in line_codes.items():
        params["LineCode"] = line_code
        
        try:
            print(f"Downloading: {description} (Line {line_code})...")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Save as CSV only
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                results = data['BEAAPI']['Results']
                if 'Data' in results:
                    csv_filename = f"{output_dir}/CAINC1_Line{line_code}_{description}.csv"
                    save_as_csv(results['Data'], csv_filename)
                    print(f"  ✓ Saved: {csv_filename}")
                else:
                    print(f"  ✗ No data available for {description}")
            else:
                print(f"  ✗ No data available for {description}")
            
            time.sleep(0.5)  # Be respectful to the API
            
        except Exception as e:
            print(f"  ✗ Error downloading {description}: {str(e)}")
    
    print(f"\nDownload complete! Files saved to '{output_dir}' directory.")


def save_as_csv(data, filename):
    """Convert JSON data to CSV format"""
    import csv
    
    if not data:
        return
    
    # Get headers from first row
    headers = list(data[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)


def download_via_web_interface(output_dir):
    """
    Alternative approach: Download using direct table requests
    This simulates the web interface interaction
    """
    print("\nAttempting to download via web interface...\n")
    
    # The URL structure for downloading tables
    download_url = "https://apps.bea.gov/regional/zip/CAINC1.zip"
    
    try:
        print("Downloading CAINC1 complete dataset...")
        response = requests.get(download_url, timeout=60)
        
        if response.status_code == 200:
            zip_file = f"{output_dir}/CAINC1_complete.zip"
            with open(zip_file, 'wb') as f:
                f.write(response.content)
            
            print(f"✓ Downloaded complete dataset to: {zip_file}")
            print("\nExtracting files...")
            
            # Extract the zip file
            import zipfile
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            
            print(f"✓ Files extracted to: {output_dir}")
            print("\nNote: The extracted files contain all CAINC1 tables.")
            
        else:
            print(f"Could not download file. Status code: {response.status_code}")
            print("\nPlease visit https://apps.bea.gov/itable/ and download manually,")
            print("or register for a free API key at https://apps.bea.gov/API/signup/")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nAlternative: Visit the BEA website directly:")
        print("1. Go to https://apps.bea.gov/itable/")
        print("2. Select 'Regional' → 'Annual State Personal Income'")
        print("3. Choose CAINC1 table")
        print("4. Click 'Download' to get the data")


if __name__ == "__main__":
    print("=" * 60)
    print("BEA CAINC1 Table Downloader")
    print("=" * 60)
    download_cainc1_tables()
