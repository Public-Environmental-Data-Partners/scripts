import os
import csv
from pathlib import Path
import pandas as pd

def get_csv_info(filepath):
    """Get information about a CSV file"""
    try:
        # Read just the header and first few rows
        df = pd.read_csv(filepath, nrows=5)
        
        info = {
            'columns': list(df.columns),
            'num_columns': len(df.columns),
            'num_rows': sum(1 for _ in open(filepath, 'r', encoding='utf-8')) - 1,  # minus header
            'sample_data': df.head(2).to_dict('records') if len(df) > 0 else []
        }
        
        # Get unique values from key columns if they exist
        df_full = pd.read_csv(filepath)
        
        if 'TimePeriod' in df_full.columns:
            info['time_periods'] = sorted(df_full['TimePeriod'].unique().tolist())
            info['years'] = sorted(set([str(tp)[:4] for tp in info['time_periods']]))
        
        if 'GeoName' in df_full.columns:
            info['num_geographies'] = df_full['GeoName'].nunique()
        
        if 'Statistic' in df_full.columns:
            info['statistic'] = df_full['Statistic'].iloc[0] if len(df_full) > 0 else 'N/A'
        
        if 'SeriesCode' in df_full.columns:
            info['num_series'] = df_full['SeriesCode'].nunique()
        
        # Get unique combinations of CL_UNIT and LineDescription
        unit_col = None
        desc_col = None
        
        # Check for different possible column names
        if 'CL_UNIT' in df_full.columns:
            unit_col = 'CL_UNIT'
        elif 'cl_unit' in df_full.columns:
            unit_col = 'cl_unit'
        
        if 'LineDescription' in df_full.columns:
            desc_col = 'LineDescription'
        elif 'Linedescription' in df_full.columns:
            desc_col = 'Linedescription'
        elif 'LineDesc' in df_full.columns:
            desc_col = 'LineDesc'
        elif 'TimeSeriesDescription' in df_full.columns:
            desc_col = 'TimeSeriesDescription'
        
        if unit_col and desc_col:
            # Create combinations
            df_full['Unit_Description'] = df_full[unit_col].astype(str) + ' - ' + df_full[desc_col].astype(str)
            unique_combos = df_full['Unit_Description'].unique().tolist()
            info['unique_descriptions'] = unique_combos
            info['num_unique_descriptions'] = len(unique_combos)
        elif desc_col:
            # If only description available
            unique_descs = df_full[desc_col].unique().tolist()
            info['unique_descriptions'] = unique_descs
            info['num_unique_descriptions'] = len(unique_descs)
        else:
            info['unique_descriptions'] = []
            info['num_unique_descriptions'] = 0
        
        return info
        
    except Exception as e:
        return {'error': str(e)}


def create_catalog(data_directory, output_file="BEA_Data_Catalog.csv"):
    """
    Create a comprehensive catalog of all downloaded BEA files
    """
    print("=" * 70)
    print("BEA DATA CATALOG GENERATOR")
    print("=" * 70)
    print()
    
    # Ask for directory if not provided
    if not data_directory or not os.path.exists(data_directory):
        print("Enter the directory containing your BEA data:")
        print("Examples:")
        print("  Current directory: bea_national_accounts")
        print("  External drive: E:\\bea_data")
        print()
        data_directory = input("Enter path: ").strip()
        
        if not data_directory:
            data_directory = "bea_national_accounts"
    
    if not os.path.exists(data_directory):
        print(f"\n✗ Error: Directory not found: {data_directory}")
        return
    
    print(f"\n✓ Scanning directory: {os.path.abspath(data_directory)}\n")
    
    # Find all CSV files
    csv_files = []
    for root, dirs, files in os.walk(data_directory):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        print("✗ No CSV files found in directory")
        return
    
    print(f"✓ Found {len(csv_files)} CSV files")
    print(f"\nAnalyzing files...\n")
    
    # Collect information about each file
    catalog_data = []
    
    for idx, filepath in enumerate(csv_files, 1):
        print(f"  [{idx}/{len(csv_files)}] Processing: {os.path.basename(filepath)}")
        
        # Get relative path from data directory
        rel_path = os.path.relpath(filepath, data_directory)
        
        # Parse filename
        filename = os.path.basename(filepath)
        filesize_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        # Get CSV info
        info = get_csv_info(filepath)
        
        # Build catalog entry
        entry = {
            'File_Path': rel_path,
            'Filename': filename,
            'Dataset': rel_path.split(os.sep)[0] if os.sep in rel_path else 'Root',
            'Subdirectory': os.path.dirname(rel_path),
            'File_Size_MB': round(filesize_mb, 2),
            'Number_of_Rows': info.get('num_rows', 'N/A'),
            'Number_of_Columns': info.get('num_columns', 'N/A'),
            'Columns': ', '.join(info.get('columns', [])),
            'Statistic': info.get('statistic', 'N/A'),
            'Number_of_Series': info.get('num_series', 'N/A'),
            'Number_of_Geographies': info.get('num_geographies', 'N/A'),
            'Year_Range': f"{info.get('years', ['N/A'])[0]} - {info.get('years', ['N/A'])[-1]}" if 'years' in info and info['years'] else 'N/A',
            'Total_Time_Periods': len(info.get('time_periods', [])) if 'time_periods' in info else 'N/A',
            'Number_of_Unique_Measures': info.get('num_unique_descriptions', 0),
            'Unique_Unit_Descriptions': ' | '.join(info.get('unique_descriptions', [])),
            'Error': info.get('error', '')
        }
        
        catalog_data.append(entry)
    
    # Save catalog to CSV
    output_path = os.path.join(data_directory, output_file)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        if catalog_data:
            writer = csv.DictWriter(f, fieldnames=catalog_data[0].keys())
            writer.writeheader()
            writer.writerows(catalog_data)
    
    print(f"\n{'='*70}")
    print(f"✓ Catalog created successfully!")
    print(f"✓ Location: {output_path}")
    print(f"{'='*70}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print("-" * 70)
    
    total_size = sum(entry['File_Size_MB'] for entry in catalog_data)
    total_rows = sum(entry['Number_of_Rows'] for entry in catalog_data if isinstance(entry['Number_of_Rows'], int))
    
    print(f"  Total Files: {len(catalog_data)}")
    print(f"  Total Size: {total_size:.2f} MB ({total_size/1024:.2f} GB)")
    print(f"  Total Rows: {total_rows:,}")
    
    # Count by dataset
    datasets = {}
    for entry in catalog_data:
        dataset = entry['Dataset']
        if dataset not in datasets:
            datasets[dataset] = 0
        datasets[dataset] += 1
    
    print(f"\n  Files by Dataset:")
    for dataset, count in sorted(datasets.items()):
        print(f"    {dataset}: {count} files")
    
    # Create summary Excel file if openpyxl is available
    try:
        import openpyxl
        excel_output = os.path.join(data_directory, "BEA_Data_Catalog.xlsx")
        
        df = pd.DataFrame(catalog_data)
        
        with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
            # Main catalog sheet
            df.to_excel(writer, sheet_name='Full Catalog', index=False)
            
            # Summary by dataset
            summary = df.groupby('Dataset').agg({
                'Filename': 'count',
                'File_Size_MB': 'sum',
                'Number_of_Rows': lambda x: x.sum() if x.dtype in ['int64', 'float64'] else 0
            }).rename(columns={'Filename': 'File_Count'})
            summary.to_excel(writer, sheet_name='Summary by Dataset')
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n✓ Excel catalog also created: {excel_output}")
        
    except ImportError:
        print("\n  (Install openpyxl for Excel format: pip install openpyxl)")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    import sys
    
    # Allow passing directory as command line argument
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    else:
        data_dir = None
    
    create_catalog(data_dir)
