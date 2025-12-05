import requests
import json

def explore_statistics():
    """
    Deep dive into how BEA structures different statistics
    """
    base_url = "https://apps.bea.gov/api/data"
    
    print("=" * 70)
    print("BEA Statistics Type Explorer")
    print("=" * 70)
    print()
    
    api_key = input("Enter your BEA API key: ").strip()
    
    if not api_key:
        print("✗ Error: API key is required!")
        return
    
    # Test multiple CAINC tables to see what "Statistic" field values appear
    test_configs = [
        {"TableName": "CAINC1", "LineCode": "1"},  # Personal income
        {"TableName": "CAINC1", "LineCode": "2"},  # Population
        {"TableName": "CAINC1", "LineCode": "3"},  # Per capita
        {"TableName": "CAINC4", "LineCode": "1"},  
        {"TableName": "CAINC30", "LineCode": "1"},
    ]
    
    print("\n1. Testing different tables to see 'Statistic' field values...")
    print("-" * 70)
    
    for config in test_configs:
        params = {
            "UserID": api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": config["TableName"],
            "LineCode": config["LineCode"],
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
                statistic = results.get('Statistic', 'N/A')
                unit = results.get('UnitOfMeasure', 'N/A')
                table = results.get('PublicTable', 'N/A')
                
                print(f"\n{config['TableName']} Line {config['LineCode']}:")
                print(f"  Statistic: {statistic}")
                print(f"  Unit: {unit}")
                print(f"  Table: {table}")
                
                # Check if there's a Statistic parameter we can use
                if 'Data' in results and len(results['Data']) > 0:
                    first_row = results['Data'][0]
                    print(f"  Sample data fields: {list(first_row.keys())}")
                    
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
    
    # Check if there's a Statistic parameter available
    print("\n\n2. Checking if 'Statistic' is a parameter we can filter by...")
    print("-" * 70)
    
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
                param_names = [p['ParameterName'] for p in results['Parameter']]
                print(f"\nAvailable parameters: {param_names}")
                
                if 'Statistic' in param_names:
                    print("\n✓ 'Statistic' IS a parameter!")
                else:
                    print("\n✗ 'Statistic' is NOT a parameter we can pass")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Check SAINC tables (State Annual Income) for comparison
    print("\n\n3. Checking SAINC tables (State version) for statistics...")
    print("-" * 70)
    
    sainc_configs = [
        {"TableName": "SAINC1", "LineCode": "1"},
        {"TableName": "SAINC1", "LineCode": "2"},
        {"TableName": "SAINC1", "LineCode": "3"},
    ]
    
    for config in sainc_configs:
        params = {
            "UserID": api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": config["TableName"],
            "LineCode": config["LineCode"],
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
                statistic = results.get('Statistic', 'N/A')
                
                print(f"\n{config['TableName']} Line {config['LineCode']}:")
                print(f"  Statistic: {statistic}")
                
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
    
    # Look for percent change specific tables
    print("\n\n4. Looking for tables with 'percent' in description...")
    print("-" * 70)
    
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
                relevant_tables = [
                    t for t in results['ParamValue'] 
                    if any(keyword in t['Desc'].lower() for keyword in ['percent', 'change', 'index', 'growth', 'rate'])
                ]
                
                if relevant_tables:
                    print("\nTables with relevant keywords:")
                    for table in relevant_tables:
                        print(f"  {table['Key']}: {table['Desc']}")
                else:
                    print("\nNo tables found with percent/change/index keywords")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    # Try GetParameterValuesFiltered to see if we can filter by statistic
    print("\n\n5. Testing GetParameterValuesFiltered with different target parameters...")
    print("-" * 70)
    
    for target in ['Statistic', 'Statistics', 'Measure', 'Type']:
        params = {
            "UserID": api_key,
            "method": "GetParameterValuesFiltered",
            "datasetname": "Regional",
            "TargetParameter": target,
            "TableName": "CAINC1",
            "ResultFormat": "JSON"
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'BEAAPI' in data:
                if 'Error' in data['BEAAPI']:
                    print(f"\n{target}: Not available")
                elif 'Results' in data['BEAAPI']:
                    results = data['BEAAPI']['Results']
                    if 'ParamValue' in results and len(results['ParamValue']) > 0:
                        print(f"\n✓ {target} IS available!")
                        print(f"  Values: {[p['Key'] for p in results['ParamValue'][:5]]}")
        except Exception as e:
            pass
    
    print("\n" + "=" * 70)
    print("Exploration complete!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Check the output above for any 'Statistic' parameter availability")
    print("2. Look for tables that contain percent change in their description")
    print("3. The different statistics might be in completely different tables")


if __name__ == "__main__":
    explore_statistics()
