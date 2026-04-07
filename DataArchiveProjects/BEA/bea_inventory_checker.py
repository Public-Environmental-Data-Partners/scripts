import os
import csv
from pathlib import Path
from collections import defaultdict
import pandas as pd

# Expected BEA tables from the readme
EXPECTED_TABLES = {
    'PISASUMMARY': 'State Annual Summary Statistics: Personal Income, GDP, Consumer Spending, Price Indexes, and Employment',
    'SAINC': 'Annual Personal Income by State',
    'SQINC': 'Quarterly Personal Income by State',
    'RPP': 'Real Personal Income and Regional Price Parities by State, MSA, and Metro/Nonmetro Portion',
    'CAINC1': 'Annual Personal Income by County',
    'CAINC4': 'Personal Income and Employment by Major Component by County',
    'CAINC5N': 'Personal Income by Major Component and Earnings by NAICS Industry',
    'CAINC5S': 'Personal Income by Major Component and Earnings by SIC Industry',
    'CAINC6N': 'Compensation of Employees by NAICS Industry',
    'CAINC6S': 'Compensation of Employees by SIC Industry',
    'CAINC30': 'Economic Profile by County',
    'CAINC91': 'Gross Flow of Earnings',
    'SAPCE': 'Personal Consumption Expenditures by State',
    'GDPSASUMMARY': 'State Annual Summary Statistics: Personal Income, GDP, Consumer Spending, Price Indexes, and Employment',
    'GDPTASUMMARY': 'U.S. Territories Annual Gross Domestic Product (GDP) Summary',
    'SAGDP': 'Annual GDP by State',
    'SQGDP': 'Quarterly GDP by State',
    'CAGDP1': 'GDP Summary by County and MSA',
    'CAGDP2': 'GDP in Current Dollars by County and MSA',
    'CAGDP8': 'Chain-type Quantity Indexes for Real GDP by County and MSA',
    'CAGDP9': 'Real GDP in Chained Dollars by County and MSA',
    'CAGDP11': 'Contributions to Percent Change in Real GDP by County and MSA',
    'SAGDP_SIC': 'Annual GDP by State by SIC Industry',
    'PRGDP': 'GDP for Puerto Rico',
    'SAACPSA': 'Arts and Culture Value Added, Compensation, and Employment by State',
    'SAORSA': 'Outdoor Recreation Value Added, Compensation, and Employment by State'
}

def scan_directory(base_path):
    """Scan a directory for BEA files and organize by type"""
    files_found = defaultdict(list)

    for root, dirs, files in os.walk(base_path):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, base_path)

            # Get file info
            file_info = {
                'path': rel_path,
                'full_path': full_path,
                'filename': file,
                'size_mb': os.path.getsize(full_path) / (1024 * 1024),
                'extension': os.path.splitext(file)[1].lower()
            }

            # Categorize by extension
            files_found[file_info['extension']].append(file_info)

    return files_found

def identify_table_files(all_files):
    """Match files to expected BEA tables"""
    table_status = {}

    for table_code, description in EXPECTED_TABLES.items():
        table_status[table_code] = {
            'description': description,
            'found': False,
            'files': [],
            'formats': []
        }

    # Check all directories for table matches
    for ext, files in all_files.items():
        for file_info in files:
            filename = file_info['filename'].upper()

            # Check if filename matches any expected table
            for table_code in EXPECTED_TABLES.keys():
                # Handle special cases
                table_search = table_code.replace('_', ' ').replace('-', ' ')

                if table_code in filename or table_search in filename:
                    table_status[table_code]['found'] = True
                    table_status[table_code]['files'].append(file_info)
                    if ext not in table_status[table_code]['formats']:
                        table_status[table_code]['formats'].append(ext)

    return table_status

def create_inventory_report(directories, output_path):
    """Create comprehensive inventory of all BEA data"""
    print("="*80)
    print("BEA DATA INVENTORY AND GAP ANALYSIS")
    print("="*80)
    print()

    # Scan all directories
    all_scans = {}
    total_files = 0
    total_size = 0

    for name, path in directories.items():
        print(f"Scanning {name}...")
        if os.path.exists(path):
            scan = scan_directory(path)
            all_scans[name] = scan

            dir_files = sum(len(files) for files in scan.values())
            dir_size = sum(f['size_mb'] for files in scan.values() for f in files)

            total_files += dir_files
            total_size += dir_size

            print(f"  Found {dir_files} files ({dir_size:.2f} MB)")
        else:
            print(f"  WARNING: Directory not found!")
            all_scans[name] = {}

    print()
    print(f"Total files across all directories: {total_files}")
    print(f"Total size: {total_size:.2f} MB ({total_size/1024:.2f} GB)")
    print()

    # Combine all files for table matching
    all_files = defaultdict(list)
    for scan in all_scans.values():
        for ext, files in scan.items():
            all_files[ext].extend(files)

    # Identify which expected tables we have
    print("Checking for expected BEA tables...")
    table_status = identify_table_files(all_files)

    found_tables = [code for code, info in table_status.items() if info['found']]
    missing_tables = [code for code, info in table_status.items() if not info['found']]

    print(f"  Found: {len(found_tables)}/{len(EXPECTED_TABLES)} expected tables")
    print(f"  Missing: {len(missing_tables)} tables")
    print()

    # Create detailed reports

    # 1. File inventory by directory
    inventory_data = []
    for dir_name, scan in all_scans.items():
        for ext, files in scan.items():
            for file_info in files:
                inventory_data.append({
                    'Directory': dir_name,
                    'Filename': file_info['filename'],
                    'Path': file_info['path'],
                    'Extension': ext,
                    'Size_MB': round(file_info['size_mb'], 2)
                })

    # 2. Table status report
    table_report = []
    for code, info in sorted(table_status.items()):
        table_report.append({
            'Table_Code': code,
            'Description': info['description'],
            'Status': 'FOUND' if info['found'] else 'MISSING',
            'File_Count': len(info['files']),
            'Formats': ', '.join(sorted(info['formats'])),
            'Files': ' | '.join([f['filename'] for f in info['files']])
        })

    # 3. File type summary
    file_type_summary = []
    extension_counts = defaultdict(lambda: {'count': 0, 'size_mb': 0})
    for ext, files in all_files.items():
        extension_counts[ext]['count'] = len(files)
        extension_counts[ext]['size_mb'] = sum(f['size_mb'] for f in files)

    for ext, stats in sorted(extension_counts.items()):
        file_type_summary.append({
            'Extension': ext,
            'File_Count': stats['count'],
            'Total_Size_MB': round(stats['size_mb'], 2)
        })

    # Save to Excel with multiple sheets
    try:
        excel_path = os.path.join(output_path, "BEA_Inventory_Report.xlsx")

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Directories Scanned',
                    'Total Files',
                    'Total Size (MB)',
                    'Total Size (GB)',
                    'Expected Tables',
                    'Tables Found',
                    'Tables Missing',
                    'Coverage %'
                ],
                'Value': [
                    len(directories),
                    total_files,
                    round(total_size, 2),
                    round(total_size / 1024, 2),
                    len(EXPECTED_TABLES),
                    len(found_tables),
                    len(missing_tables),
                    f"{len(found_tables)/len(EXPECTED_TABLES)*100:.1f}%"
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

            # Table status
            pd.DataFrame(table_report).to_excel(writer, sheet_name='Table Status', index=False)

            # File inventory
            pd.DataFrame(inventory_data).to_excel(writer, sheet_name='File Inventory', index=False)

            # File type summary
            pd.DataFrame(file_type_summary).to_excel(writer, sheet_name='File Types', index=False)

            # Missing tables detail
            missing_detail = [
                {
                    'Table_Code': code,
                    'Description': EXPECTED_TABLES[code]
                }
                for code in sorted(missing_tables)
            ]
            pd.DataFrame(missing_detail).to_excel(writer, sheet_name='Missing Tables', index=False)

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
                    adjusted_width = min(max_length + 2, 80)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

        print(f"Excel report created: {excel_path}")

    except Exception as e:
        print(f"Error creating Excel: {e}")

    # Also save CSV versions
    csv_path = os.path.join(output_path, "BEA_Table_Status.csv")
    pd.DataFrame(table_report).to_csv(csv_path, index=False)
    print(f"CSV report created: {csv_path}")

    csv_path = os.path.join(output_path, "BEA_File_Inventory.csv")
    pd.DataFrame(inventory_data).to_csv(csv_path, index=False)
    print(f"CSV inventory created: {csv_path}")

    # Print results to console
    print()
    print("="*80)
    print("FOUND TABLES:")
    print("="*80)
    for code in sorted(found_tables):
        info = table_status[code]
        print(f"{code:15} - {info['description']}")
        print(f"                Files: {', '.join([f['filename'] for f in info['files']])}")
        print()

    if missing_tables:
        print()
        print("="*80)
        print("MISSING TABLES:")
        print("="*80)
        for code in sorted(missing_tables):
            print(f"{code:15} - {EXPECTED_TABLES[code]}")
        print()

    print("="*80)
    print("FILE TYPE BREAKDOWN:")
    print("="*80)
    for ext, stats in sorted(extension_counts.items()):
        print(f"{ext:10} - {stats['count']:4} files ({stats['size_mb']:8.2f} MB)")

    print()
    print("="*80)
    print(f"Reports saved to: {output_path}")
    print("="*80)

    return table_status, inventory_data

if __name__ == "__main__":
    # Define directories to scan
    directories = {
        'BEA_CountyMetroLocal': r"D:\data_archives_2025\BEA_CountyMetroLocal",
        'BEA_National': r"D:\data_archives_2025\BEA_National",
        'BEA_State': r"D:\data_archives_2025\BEA_State"
    }

    # Output directory (use the first existing directory)
    output_dir = None
    for path in directories.values():
        if os.path.exists(path):
            output_dir = os.path.dirname(path)
            break

    if not output_dir:
        output_dir = os.getcwd()

    table_status, inventory = create_inventory_report(directories, output_dir)
