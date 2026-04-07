# sampleGhgDBFiles.py --
# When we weren't sure how enormous the GHG database was going to be,
# I constructed this script to download up to the first 100 records 
# of each table so we could get a size estimate.
# See downloadGhgDbFilesViaJson.py for more information.

import csv
import json
import os
import requests
import shutil
import time

# Global variable for the log file object (re-using from previous turns)
LOG = None
# Global variable for the api endpoint base URL
API_ENDPOINT = "https://data.epa.gov/dmapservice/query"

def make_download_folder(download_folder):
    result = True
    # Ensure the download folder is clean each time
    try:
        if os.path.exists(download_folder):
            # Delete the folder and all its contents
            shutil.rmtree(download_folder)
            print(f"Existing folder '{download_folder}' and its contents removed.", file=LOG)

        # Re-create the empty folder
        os.makedirs(download_folder)
        print(f"Folder '{download_folder}' created/re-created successfully and is now empty.", file=LOG)
    except OSError as e:
        print(f"***Error managing folder '{download_folder}': {e}", file=LOG)
        result = False
    return result

def fetch_table_data(table_name: str, record_limit: int):
    """
    Fetches all columns data for a given GHG table from the EPA DMAP service.

    Args:
        table_name (str): The name of the table to fetch data from (e.g., "rlps_ghg_emitter_facilities").
        url (str): The URL of the GraphQL endpoint.
                   Defaults to "https://data.epa.gov/dmapservice/query".

    Returns:
        dict or None: A dictionary containing the table data if successful, None otherwise.
    """
    global LOG # Declare intent to use the global LOG
    #print(f"Attempting to fetch data for table: {table_name}", file=LOG)
    table_data = None

    # Construct the GraphQL query string using an f-string
    # The __all_columns__ directive tells GraphQL to return all available columns.
    graphql_query_string = f"""
    query fieldsQuery {{
        ghg__{table_name} (limit: {record_limit}){{
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
    LOG_name = "sample.log"

    # Open the log file at the beginning of main()
    try:
        LOG = open(LOG_name, "w", encoding="utf-8")
        print(f"Log file '{LOG_name}' opened successfully.", file=LOG)
    except IOError as e:
        print(f"Error opening log file: {e}")
        LOG = None
        return # Exit if we can't open the log file

    start_time = time.time()
    print(f"Script started at: {time.ctime(start_time)}", file=LOG)
    print("-" * 30, file=LOG)

    download_folder = "sampleData"
    if (make_download_folder(download_folder) != True):
        exit(1)

    # For now, we are trying to estimate a total db size,
    # we only want a sample of the records
    record_limit = 100

    table_list_path = "ghgTableCountSorted.csv"
    print(f"Reading table list from {table_list_path}...", file=LOG)
    try:
        with open(table_list_path, mode='r', newline='', encoding='utf-8') as listfile:
            # Use csv.DictReader to read rows as dictionaries, where keys are column headers
            reader = csv.DictReader(listfile)
            # Check if the required headers exist
            if 'table_name' not in reader.fieldnames or 'record_count' not in reader.fieldnames:
                print(f"Error: CSV file is missing required columns. Expected 'table_name' and 'record_count'. Found: {reader.fieldnames}", file=LOG)
                exit(1)
            estimated_total_size = 0
            for i, row in enumerate(reader):
                # 'row' is a dictionary like {'table_id': '368', 'table_name': 'ef_w_onshore_wells', ...}
                # Extract the values into your variables
                table_name_from_csv = row['table_name']
                # This is intentionally printed to the console as a heartbeat indicator
                if (i % 10 == 0):
                    print("about to process table #" + str(i+1), ":", table_name_from_csv)
                # Convert record_count to an integer as it comes as a string from CSV
                total_record_count_from_csv = int(row['record_count'])
                print(f"CSV Row {i+1}: Table Name='{table_name_from_csv}', Record Count={total_record_count_from_csv}", file=LOG)
                output_path = f"{download_folder}/{table_name_from_csv}_sample.csv"
                downloaded_data = fetch_table_data(table_name_from_csv, record_limit)
                if downloaded_data:
                    actual_record_count = len(downloaded_data)   # not all files have 100
                    estimated_ratio = 1.0
                    if (actual_record_count > 0) :
                        estimated_ratio = float(total_record_count_from_csv) / float(actual_record_count)
                        print("actual record count:", actual_record_count,
                              "total_record_count_from_csv:", total_record_count_from_csv,
                              "ratio:", estimated_ratio, file=LOG)
                    write_csv_file(downloaded_data, output_path, table_name_from_csv)
                    actual_file_size = os.path.getsize(output_path)
                    estimated_file_size = actual_file_size * estimated_ratio
                    print("downloaded filesize:", actual_file_size, "estimated file size:", estimated_file_size, file=LOG)
                    estimated_total_size = estimated_total_size + estimated_file_size
                else:
                    print(f"Failed to download data for {table_name_from_csv}.", file=LOG)
    except FileNotFoundError:
        print(f"Error: The CSV file '{table_list_path}' was not found. Please check the path.", file=LOG)
    except KeyError as e:
        print(f"Error: A required column '{e}' was not found in the CSV file. Check CSV headers.", file=LOG)
    except ValueError as e:
        print(f"Error: Could not convert 'record_count' to an integer for a row. Details: {e}", file=LOG)
    except Exception as e:
        print(f"An unexpected error occurred while reading the CSV: {e}", file=LOG)

    print(i, "files processed.", file=LOG)
    est_mb = round(estimated_total_size / 1024 / 1024, 2)
    print(f"estimated total download size: {est_mb} MB", file=LOG)

    print("-" * 30, file=LOG)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Script finished at: {time.ctime(end_time)}", file=LOG)
    print(f"Total elapsed execution time: {elapsed_time:.2f} seconds", file=LOG)

    # Close the log file
    if LOG:
        LOG.close()
        # This final print will go to the console as the LOG is now closed
        print(f"Log file '{LOG_name}' closed.")


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