import requests
import json

def explore_regional_parameters():
    """
    Explore what parameters are available for Regional dataset
    """
    base_url = "https://apps.bea.gov/api/data"
    
    print("=" * 70)
    print("BEA Regional Dataset Parameter Explorer")
    print("=" * 70)
    print()
    
    api_key = input("Enter your BEA API key: ").strip()
    
    if not api_key:
        print("✗ Error: API key is required!")
        return
    
    # Get all parameters for Regional dataset
    print("\n1. Getting all parameters for Regional dataset...")
    params = {
        "UserID": api_key,
        "method": "GetParameterList",
        "datasetname": "Regional",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'Parameter' in results:
                print("\nAvailable Parameters:")
                print("-" * 70)
                for param in results['Parameter']:
                    print(f"\nParameter: {param['ParameterName']}")
                    print(f"  Description: {param['ParameterDescription']}")
                    print(f"  DataType: {param['ParameterDataType']}")
                    print(f"  Required: {param['ParameterIsRequiredFlag']}")
                    print(f"  MultipleAccepted: {param.get('MultipleAcceptedFlag', 'N/A')}")
                    if 'AllValue' in param:
                        print(f"  AllValue: {param['AllValue']}")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return
    
    # Now check what's available for CAINC1 specifically
    print("\n\n2. Checking available line codes for CAINC1...")
    params = {
        "UserID": api_key,
        "method": "GetParameterValuesFiltered",
        "datasetname": "Regional",
        "TargetParameter": "LineCode",
        "TableName": "CAINC1",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'ParamValue' in results:
                print(f"\nFound {len(results['ParamValue'])} line codes for CAINC1:")
                print("-" * 70)
                for item in results['ParamValue'][:20]:  # Show first 20
                    print(f"  {item['Key']}: {item['Desc']}")
                if len(results['ParamValue']) > 20:
                    print(f"  ... and {len(results['ParamValue']) - 20} more")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Check what tables are available
    print("\n\n3. Checking all Regional tables (looking for CAINC variants)...")
    params = {
        "UserID": api_key,
        "method": "GetParameterValues",
        "datasetname": "Regional",
        "ParameterName": "TableName",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            if 'ParamValue' in results:
                cainc_tables = [t for t in results['ParamValue'] if 'CAINC' in t['Key']]
                print(f"\nFound {len(cainc_tables)} CAINC tables:")
                print("-" * 70)
                for table in cainc_tables:
                    print(f"  {table['Key']}: {table['Desc']}")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Test a specific data call to see what fields are returned
    print("\n\n4. Testing a sample data call to see response structure...")
    params = {
        "UserID": api_key,
        "method": "GetData",
        "datasetname": "Regional",
        "TableName": "CAINC1",
        "LineCode": "1",
        "GeoFips": "STATE",
        "Year": "2023",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            results = data['BEAAPI']['Results']
            
            print("\nResponse structure:")
            print("-" * 70)
            print("Results keys:", list(results.keys()))
            
            if 'Data' in results and len(results['Data']) > 0:
                print("\nFirst data row fields:")
                first_row = results['Data'][0]
                for key, value in first_row.items():
                    print(f"  {key}: {value}")
            
            # Save full response for inspection
            with open('bea_sample_response.json', 'w') as f:
                json.dump(data, f, indent=2)
            print("\n✓ Full response saved to 'bea_sample_response.json'")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    print("\n" + "=" * 70)
    print("Exploration complete!")
    print("=" * 70)


if __name__ == "__main__":
    explore_regional_parameters()
