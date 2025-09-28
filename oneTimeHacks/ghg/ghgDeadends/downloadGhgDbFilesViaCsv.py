import csv
import io
import json
import os
import requests
import shutil
import time

# Global variable for the log file object (re-using from previous turns)
LOG = None
# Global variable for the api endpoint base URL, requesting data in csv format, not json
API_ENDPOINT = "https://data.epa.gov/dmapservice/query/csv"

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
    global LOG # Declare intent to use the global LOG
    print(f"Attempting to fetch data for table: {table_name}", file=LOG, flush=True)
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
        print("About to transmit post", file=LOG, flush=True)
        response = requests.post(API_ENDPOINT, json=payload)
        # Send the POST request using the 'json' parameter.
        # requests will automatically set Content-Type: application/json
        response = requests.post(API_ENDPOINT, json=payload)

        # Raise an HTTPError for bad responses (4xx or 5xx)
        response.raise_for_status()
        # Optionally, inspect content-type header for debugging
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/csv' in content_type:
            print(f"Received 'text/csv' content for {table_name}.", file=LOG, flush=True)
        else:
            print(f"WARNING: Expected 'text/csv' but received '{content_type}' for {table_name}. "
                  f"Raw response snippet: {response.text[:200]}...", file=LOG, flush=True)

        # Parse the csv response
        raw_csv_content = response.text
        print("Back from post, appear to have csv content", file=LOG, flush=True)
        csv_reader = csv.reader(io.StringIO(raw_csv_content))
        table_data = list(csv_reader)
        # Assuming the first row is always a header, let's see it
        #print("header row:", table_data[0], file=LOG, flush=True)
        number_of_data_rows = len(table_data) - 1
        print(f"Have {number_of_data_rows}  records in downloaded CSV content.", file=LOG, flush=True)

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
    LOG_name = "downloadViaCsv.log"

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

    download_folder = "downloadCsv"
    if (make_download_folder(download_folder) != True):
        print("Could not create download folder. Aborting.", file=LOG)
        exit(1)

    table_list_path = "../ghgTableCountTest.csv"
    # table_list_path = "ghgTableCountSorted.csv" # largest to smallest record count
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
                output_path = f"{download_folder}/{table_name_from_csv}_sample.csv"
                # Don't download the same file multiple times
                if os.path.exists(output_path):
                    print(output_path, "already exists, skipping the download", file=LOG)
                    continue
                else:
                    downloaded_data = fetch_table_data(table_name_from_csv)
                    if downloaded_data:
                        # TODO, skip for now write_csv_file(downloaded_data, output_path, table_name_from_csv)
                        if len(downloaded_data) != expected_record_count:
                            print(f"*** WARNING: expected {expected_record_count} records, fetched {len(downloaded_data)} records instead", file=LOG, flush=True)
                    else:
                        print(f"Failed to download data for {table_name_from_csv}.", file=LOG, flush=True)
                # let's not beat the api too hard, wait a second between tables.
                time.sleep(1)
                print("back from sleep", file=LOG, flush=True)
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