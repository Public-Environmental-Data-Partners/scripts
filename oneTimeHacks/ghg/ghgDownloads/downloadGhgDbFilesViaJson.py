# downloadGhgDbFilesViaJson.py
# This script downloads greenhouse gas (GHG) data tables from the EPA DMAP API.
# It reads a sorted CSV file that lists tables and their record counts.
# The csv file is a sorted version of the ghgTableCount.csv file produced by
# ghgModelFromDMAPapi.py. The script iterates through each table in the list,
# then fetches each table's data using a GraphQL query.
# Downloaded data is converted from JSON to CSV files stored in the `download` folder.
# Empty tables fail benignly, logging and error but not causing the script to stop.
# Progress and errors are logged to `download.log`. The script pausesbetween downloads to
# avoid overloading the API.


import csv
import json
import os
import requests
import time

# Global variables
LOG = None
API_ENDPOINT = "https://data.epa.gov/dmapservice/query"
# This code runs off a sorted version of the ghgTableCount.csv
# file produced by ghgModelFromDMAPapi.py, sorted in reverse
# order by record count. The code would run equally well from
# the original file but I wanted to see/watch the biggest
# files as they downloaded so I would have some expectation of
# the time the whole process would take. 
TABLE_LIST_FILENAME = "ghgTableCountSorted.csv"
DOWNLOAD_FOLDER = "download"
LOG_FILENAME = "download.log"

def make_download_folder(download_folder):
    result = True
    try:
        if not os.path.exists(download_folder):
            # Re-create the empty folder
            os.makedirs(download_folder)
            print(f"Folder '{download_folder}' created.", file=LOG)
        else:
            print(f"Folder '{download_folder}' already exists.", file=LOG)
    except OSError as e:
        print(f"***Error managing folder '{download_folder}': {e}", file=LOG)
        result = False
    return result

def fetch_table_data(table_name: str):
    print(f"Attempting to fetch data for table '{table_name}'...", file=LOG)
    """
    Fetches all columns data for a given GHG table from the EPA DMAP service.

    Args:
        table_name (str): The name of the table to fetch data from (e.g., "rlps_ghg_emitter_facilities").
        url (str): The URL of the GraphQL endpoint.
                   Defaults to "https://data.epa.gov/dmapservice/query".

    Returns:
        dict or None: A dictionary containing the table data if successful, None otherwise.
    """
    #print(f"Attempting to fetch data for table: {table_name}", file=LOG)
    table_data = None

    # Construct the GraphQL query string using an f-string
    # The __all_columns__ directive tells GraphQL to return all available columns.
    graphql_query_string = f"""
    query fieldsQuery {{
        ghg__{table_name} {{
            __all_columns__
        }}
    }}
    """

    # Create the Python dictionary payload for the requests library
    payload = {
        "query": graphql_query_string
    }

    try:
        # Send the POST request using the 'json' parameter.
        # requests will automatically set Content-Type: application/json
        response = requests.post(API_ENDPOINT, json=payload)

        # Raise an HTTPError for bad responses (4xx or 5xx)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        # Check if the data key exists and contains the table data
        if "data" in data and f"ghg__{table_name}" in data["data"]:
            table_data = data["data"][f"ghg__{table_name}"]
            print(f"Successfully fetched {len(table_data)} records for {table_name}.", file=LOG)
        else:
            print(f"No data found for {table_name} in the response. Full response: {json.dumps(data, indent=2)}", file=LOG)

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching data for {table_name} (Status {e.response.status_code}): {e.response.text}", file=LOG)
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error fetching data for {table_name}: {e}", file=LOG)
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error fetching data for {table_name}: {e}", file=LOG)
    except requests.exceptions.RequestException as e:
        print(f"An unexpected Request Error occurred for {table_name}: {e}", file=LOG)
    except json.JSONDecodeError:
        print(f"Failed to decode JSON response for {table_name}. Raw response: {response.text}", file=LOG)
    except Exception as e:
        print(f"An unhandled error occurred while fetching data for {table_name}: {e}", file=LOG)

    return table_data

def main():
    global LOG

    # Open the log file at the beginning of main()
    try:
        LOG = open(LOG_FILENAME, "w", encoding="utf-8")
        print(f"Log file '{LOG_FILENAME}' opened successfully.", file=LOG)
    except IOError as e:
        print(f"Error opening log file: {e}")
        LOG = None
        return # Exit if we can't open the log file

    start_time = time.time()
    print(f"Script started at: {time.ctime(start_time)}", file=LOG)
    print("-" * 30, file=LOG)


    if (make_download_folder(DOWNLOAD_FOLDER) != True):
        exit(1)

    print(f"Reading table list from {TABLE_LIST_FILENAME}...", file=LOG)
    try:
        with open(TABLE_LIST_FILENAME, mode='r', newline='', encoding='utf-8') as listfile:
            # Use csv.DictReader to read rows as dictionaries, where keys are column headers
            reader = csv.DictReader(listfile)
            # Check if the required headers exist
            if 'table_name' not in reader.fieldnames or 'record_count' not in reader.fieldnames:
                print(f"Error: CSV file is missing required columns. Expected 'table_name' and 'record_count'. Found: {reader.fieldnames}", file=LOG)
                exit(1)
            for i, row in enumerate(reader):
                # 'row' is a dictionary like {'table_id': '368', 'table_name': 'ef_w_onshore_wells', ...}
                # Extract the values into your variables
                table_name_from_csv = row['table_name']
                # Convert record_count to an integer as it comes as a string from CSV
                expected_record_count = int(row['record_count'])
                # This is intentionally printed to the console as a heartbeat indicator
                if (i % 10 == 0):
                    print("about to process table #" + str(i+1), ":", table_name_from_csv)
                print(f"\nCSV Row {i+1}: Table Name='{table_name_from_csv}', Record Count={expected_record_count}", file=LOG, flush=True)
                output_path = f"{DOWNLOAD_FOLDER}/{table_name_from_csv}.csv"
                # Don't download the same file multiple times
                if os.path.exists(output_path):
                    print(output_path, "already exists, skipping the download", file=LOG)
                    continue
                else:
                    downloaded_data = fetch_table_data(table_name_from_csv)
                    if downloaded_data:
                        write_csv_file(downloaded_data, output_path, table_name_from_csv)
                        if len(downloaded_data) != expected_record_count:
                            print(f"*** WARNING: expected {expected_record_count} records, fetched {len(downloaded_data)} records instead", file=LOG)
                    else:
                        print(f"Failed to download data for {table_name_from_csv}.", file=LOG)
                # let's not beat the api too hard, wait a second between tables.
                time.sleep(1)
    except FileNotFoundError:
        print(f"Error: The CSV file '{TABLE_LIST_FILENAME}' was not found. Please check the path.", file=LOG)
    except KeyError as e:
        print(f"Error: A required column '{e}' was not found in the CSV file. Check CSV headers.", file=LOG)
    except ValueError as e:
        print(f"Error: Could not convert 'record_count' to an integer for a row. Details: {e}", file=LOG)
    except Exception as e:
        print(f"An unexpected error occurred while reading the CSV: {e}", file=LOG)

    print(i, "files processed.", file=LOG)

    print("-" * 30, file=LOG)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Script finished at: {time.ctime(end_time)}", file=LOG)
    print(f"Total elapsed execution time: {elapsed_time:.2f} seconds", file=LOG)

    # Close the log file
    if LOG:
        LOG.close()
        # This final print will go to the console as the LOG is now closed
        print(f"Log file '{LOG_FILENAME}' closed.")


def write_csv_file(downloaded_data, output_path, target_table_name):
    try:
        # Get the header row from the keys of the first record
        # This assumes all records have the same keys, which is generally true for GraphQL __all_columns__
        fieldnames = list(downloaded_data[0].keys())
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write the header row
            writer.writerows(downloaded_data)  # Write all data rows

        #print(f"Sample data for {target_table_name} saved to {output_path} as CSV.", file=LOG)
    except IOError as e:
        print(f"Error saving data to {output_path}: {e}", file=LOG)
    except Exception as e:  # Catch other potential errors during CSV writing
        print(f"An unexpected error occurred during CSV writing for {target_table_name}: {e}", file=LOG)


if __name__ == "__main__":
    main()