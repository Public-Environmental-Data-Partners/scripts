BEA Python Scripts Readme



Downloader Scripts (4 scripts - these actually download data)

bea\_CAINC1\_downloader.py - Downloads CAINC1 tables (Personal Income by County) via BEA API

bea\_cainc1\_metro\_dl.py - Downloads CAINC1 data specifically for metro areas using BEA API

bea\_cainc\_all\_stats.py - Downloads all CAINC-related statistics (multiple table types) via BEA API

bea\_national\_acct\_download.py - Downloads national accounts data from BEA API (for the BEA\_National directory)



Analysis/Utility Scripts (3 scripts - these analyze/explore data)

bea\_catalog\_generator.py - Generates a catalog/index of downloaded BEA CSV files with metadata

bea\_explore.py - Explores BEA API parameters and available datasets

bea\_explore\_stats.py - Tests different tables to understand the 'Statistic' field values

bea\_inventory\_checker.py - Checks the archive against expected tables and creates comprehensive inventory reports



Support File

All the downloader scripts use the BEA API with an API key to fetch data and save it as CSV files.



Scripts That Show Comprehensive Table Lists

1\. bea\_explore.py (lines 81-104)

* Gets all Regional dataset tables using GetParameterValues with ParameterName: TableName
* Filters to show CAINC tables specifically
* Shows table codes and descriptions
* bea\_explore.py is specifically designed for exploration and 1) Lists all parameters for Regional datasets; 2) Shows all available CAINC table codes; 3) Tests sample data calls
* 



2\. bea\_national\_acct\_download.py (lines 31-89)

* Has get\_all\_datasets() function that calls GetDataSetList to get all BEA datasets
* Has get\_tables\_for\_dataset() function that gets all tables for NIPA, NIUnderlyingDetail, and FixedAssets datasets
* bea\_national\_acct\_download.py 1) Lists all available BEA datasets (NIPA, Regional, FixedAssets, etc.), 2) Gets all tables within specific datasets, 3) Actually downloads the data
