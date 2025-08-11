import requests
import csv
import sys
import time

# --- Global file handle for logging ---
# This will be assigned the file object in the main() function.
LOG_FILE = None

# --- Constants ---
SEARCH_API_URL = "https://dataverse.harvard.edu/api/search"
NATIVE_API_URL = "https://dataverse.harvard.edu/api/datasets/:persistentId"
CSV_FILENAME = "cafe_dataverse_metadata.csv"
BASE_DATASET_URL = "https://dataverse.harvard.edu/dataset.xhtml?persistentId="

def discover_datasets(search_url):
    """
    Step 1: Discovers dataset DOIs using the Dataverse Search API.
    All output is sent to the global log_file.
    """
    print("Step 1: Discovering datasets using the Search API...", file=LOG_FILE)
    all_dataset_doi_list = []
    search_params = {
        "q": "*",
        "type": "dataset",
        "subtree": "cafe-extracted-data",
        "per_page": 1000
    }

    try:
        response = requests.get(search_url, params=search_params)
        response.raise_for_status()
        search_data = response.json()

        if search_data.get("status") == "OK":
            items = search_data.get("data", {}).get("items", [])
            for item in items:
                if item.get("global_id"):
                    all_dataset_doi_list.append(item["global_id"])
            print(f"Found {len(all_dataset_doi_list)} datasets to process.", file=LOG_FILE)
            return all_dataset_doi_list
        else:
            print(f"Error from Search API: {search_data.get('message', 'Unknown error')}", file=LOG_FILE)
            sys.exit(1)

    except requests.exceptions.RequestException as e:
        print(f"Network error during Search API call: {e}", file=LOG_FILE)
        sys.exit(1)
    except ValueError as e:
        print(f"Error decoding JSON from Search API: {e}", file=LOG_FILE)
        sys.exit(1)


def fetch_detailed_metadata(doi_list, native_url, base_dataset_url):
    """
    Step 2: Retrieves and processes detailed metadata for each dataset DOI.
    All output is sent to the global log_file.
    """
    print("\nStep 2: Retrieving detailed metadata for each dataset...", file=LOG_FILE)
    dataset_metadata_list = []

    num_entries = len(doi_list)
    for i, doi in enumerate(doi_list):
        if (i > 0 and i % 10 == 0):
            # print a heartbeat indicator to the console since this is
            # a very long running process
            print(f"--- Completed processing for {i} of {num_entries} datasets ---")
        try:
            native_api_params = {"persistentId": doi}
            response = requests.get(native_url, params=native_api_params)
            response.raise_for_status()
            native_data = response.json()

            if native_data.get("status") == "OK":
                dataset_data = native_data.get("data", {})
                latest_version = dataset_data.get("latestVersion", {})
                metadata_blocks = latest_version.get("metadataBlocks", {})
                citation_fields = metadata_blocks.get("citation", {}).get("fields", [])

                title = next((f.get("value") for f in citation_fields if f.get("typeName") == "title"), "N/A")
                depositor = next((f.get("value") for f in citation_fields if f.get("typeName") == "depositor"),
                                 "unknown")

                dataset_metadata_list.append({
                    "Title": title,
                    "Publication Date": latest_version.get("releaseTime", "N/A"),
                    "Depositor": depositor,
                    "Date of Deposit": latest_version.get("createTime", "N/A"),
                    "CAFE URL": base_dataset_url + doi
                })
            else:
                print(f"API Error for DOI {doi}: {native_data.get('message', 'Unknown error')}", file=LOG_FILE)

        except requests.exceptions.RequestException as e:
            print(f"Network error for DOI {doi}: {e}", file=LOG_FILE)
        except ValueError as e:
            print(f"JSON decode error for DOI {doi}: {e}", file=LOG_FILE)
        except Exception as e:
            print(f"An unexpected error occurred for DOI {doi}: {e}", file=LOG_FILE)

        time.sleep(1)

    return dataset_metadata_list


def generate_csv(metadata_list, filename):
    """
    Step 3: Writes the collected metadata to a CSV file.
    All output is sent to the global log_file.
    """
    print(f"\nStep 3: Generating CSV file '{filename}'...", file=LOG_FILE)
    if not metadata_list:
        print("No metadata was collected; CSV file will not be generated.", file=LOG_FILE)
        return

    fieldnames = list(metadata_list[0].keys())
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(metadata_list)
        print(f"Success! CSV file generated with {len(metadata_list)} entries. ✅", file=LOG_FILE)
    except IOError as e:
        print(f"Error writing to CSV file: {e}", file=LOG_FILE)
    except Exception as e:
        print(f"An unexpected error occurred while writing the CSV: {e}", file=LOG_FILE)


def main():
    """
    Main function to open the log file and orchestrate the script execution.
    """
    global LOG_FILE
    try:
        # The 'with' statement ensures the file is properly closed even if errors occur.
        with open("generateAuditList.log", "w", encoding="utf-8") as f:
            LOG_FILE = f
            print(f"Log started at {time.ctime()}", file=LOG_FILE)

            # Step 1: Get all dataset DOIs
            doi_list = discover_datasets(SEARCH_API_URL)
            if not doi_list:
                print("No datasets found. Exiting.", file=LOG_FILE)
                return

            # Step 2: Fetch detailed metadata for each DOI
            detailed_metadata = fetch_detailed_metadata(doi_list, NATIVE_API_URL, BASE_DATASET_URL)

            # Step 3: Write the collected data to a CSV file
            generate_csv(detailed_metadata, CSV_FILENAME)

            print(f"\nLog finished at {time.ctime()}", file=LOG_FILE)

    except IOError as e:
        # This print statement is for the console, as it will only run if the log file
        # itself cannot be opened, which is a critical failure.
        print(f"FATAL ERROR: Could not open or write to log file 'generateAuditList.log'.\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()