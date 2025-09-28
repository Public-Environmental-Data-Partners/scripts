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
DOWNLOAD_FOLDER = "downloadTest"
LOG_FILENAME = "../downloadTest.log"

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


# --- Helper function to execute a single GraphQL query ---
def execute_graphql_query(table_name: str, limit: int = None) -> dict | list | None:
    """
    Executes a single GraphQL query to the EPA DMAP service.

    Args:
        table_name (str): The name of the table to query (e.g., "ghg__rlps_ghg_emitter_facilities").
        limit (int, optional): The limit for records. If None, no limit is applied.

    Returns:
        dict | list | None: The 'data' part of the response (which could be a list of records
                            or a dictionary if it contains other fields), or None if an error occurs.
    """
    query_limit_clause = f"(limit: {limit})" if limit is not None else ""

    graphql_query_string = f"""
    query fieldsQuery {{
        {table_name} {query_limit_clause} {{
            __all_columns__
        }}
    }}
    """

    payload = {
        "query": graphql_query_string
    }

    print(f"  Attempting GraphQL query for {table_name} with limit: {limit if limit is not None else 'None'}", file=LOG)
    print(f"  Query payload: {json.dumps(payload, indent=2)}", file=LOG) # Uncomment for detailed query debugging

    try:
        response = requests.post(API_ENDPOINT, json=payload,
                                 timeout=30)  # Increased timeout for potentially large downloads
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        # Check for GraphQL errors in the response
        if 'errors' in data:
            print(f"  API returned errors for {table_name}: {data['errors']}", file=LOG)
            # Even with errors, sometimes 'data' might still be present, so we proceed to check 'data'

        if "data" in data and table_name in data["data"]:
            return data["data"][table_name]
        else:
            print(
                f"  No 'data' or specific table data found in response for {table_name}. Full response keys: {data.keys()}",
                file=LOG)
            return None  # Indicate that data was not found in the expected structure

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP Error for {table_name} (Status {e.response.status_code}): {e.response.text}", file=LOG)
    except requests.exceptions.ConnectionError as e:
        print(f"  Connection Error for {table_name}: {e}", file=LOG)
    except requests.exceptions.Timeout as e:
        print(f"  Timeout Error for {table_name}: {e}", file=LOG)
    except requests.exceptions.RequestException as e:
        print(f"  An unexpected Request Error occurred for {table_name}: {e}", file=LOG)
    except json.JSONDecodeError:
        print(f"  Failed to decode JSON response for {table_name}. Raw response: {response.text[:500]}...", file=LOG)
    except Exception as e:
        print(f"  An unhandled error occurred during query for {table_name}: {e}", file=LOG)

    return None  # Return None if any exception occurs or data not found


def fetch_table_data(table_name: str) -> list | None:
    """
    Fetches all columns data for a given GHG table from the EPA DMAP service.
    First tries without a limit. If that returns 0 records, then tries with limit:0.

    Args:
        table_name (str): The name of the table to fetch data from (e.g., "ghg__rlps_ghg_emitter_facilities").

    Returns:
        list or None: A list of records (even if empty) containing the table data,
                      or None if both attempts fail or no schema can be inferred.
    """
    print(f"Attempting to fetch data for table: {table_name}", file=LOG)

    # Attempt 1: Fetch without any limit
    print(f"  Trying to fetch full data for {table_name}...", file=LOG)
    table_data_full = execute_graphql_query(table_name)

    if table_data_full is None:
        print(f"  Initial fetch for {table_name} failed or returned no data structure. Proceeding to fallback.",
              file=LOG)
        # Proceed to fallback attempt

    elif isinstance(table_data_full, list) and len(table_data_full) > 0:
        print(f"  Successfully fetched {len(table_data_full)} records for {table_name} (no limit).", file=LOG)
        return table_data_full  # Data found, return it

    elif isinstance(table_data_full, list) and len(table_data_full) == 0:
        print(f"  Initial fetch for {table_name} returned 0 records. Trying with limit:0 to get column names...",
              file=LOG)
        # Proceed to fallback attempt

    else:  # Unexpected data type from initial fetch
        print(
            f"  Unexpected data type returned for {table_name} in initial fetch: {type(table_data_full)}. Proceeding to fallback.",
            file=LOG)

    # Attempt 2: If no records found in the first attempt, or initial fetch failed, try with limit:0
    table_data_schema_only = execute_graphql_query(table_name, limit=0)

    if table_data_schema_only is None:
        print(f"  Fallback fetch with limit:0 for {table_name} also failed or returned no data structure.", file=LOG)
        return None  # Both attempts failed to get data or schema structure
    elif isinstance(table_data_schema_only, list) and len(table_data_schema_only) > 0:
        # This means limit:0 actually returned some records (unexpected but possible, or a single schema object)
        print(
            f"  Fallback fetch with limit:0 for {table_name} returned {len(table_data_schema_only)} records/schema objects.",
            file=LOG)
        return table_data_schema_only
    elif isinstance(table_data_schema_only, list) and len(table_data_schema_only) == 0:
        print(
            f"  Fallback fetch with limit:0 for {table_name} returned 0 records. Cannot infer column names from data.",
            file=LOG)
        return None  # No records and no schema to infer from an empty list
    else:  # Unexpected data type from fallback fetch
        print(f"  Fallback fetch for {table_name} returned unexpected type: {type(table_data_schema_only)}", file=LOG)
        return None

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

    #table_list_path = "ghgTableCountSorted.csv"
    table_list_path = "../ghgTableCountTest.csv"
    print(f"Reading table list from {table_list_path}...", file=LOG)
    try:
        with open(table_list_path, mode='r', newline='', encoding='utf-8') as listfile:
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
                print(f"CSV Row {i+1}: Table Name='{table_name_from_csv}', Record Count={expected_record_count}", file=LOG)
                output_path = f"{DOWNLOAD_FOLDER}/{table_name_from_csv}_sample.csv"
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
        print(f"Error: The CSV file '{table_list_path}' was not found. Please check the path.", file=LOG)
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