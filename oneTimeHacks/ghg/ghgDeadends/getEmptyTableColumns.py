import requests
from bs4 import BeautifulSoup
import os
import time
import csv

# --- Constants ---
BASE_URL = "https://enviro.epa.gov/envirofacts/metadata/table/ghg/"
OUTPUT_DIR = "downloadEmpties"
SLEEP_TIME = 1  # seconds
INPUT_FILE_NAME = "../tableNamesTest.txt"  # New constant for the input file name


# --- Step 1: Create Output Directory ---
def create_output_directory(directory_name: str):
    """
    Creates the specified output directory if it does not already exist.

    Args:
        directory_name (str): The name of the directory to create.
    """
    print(f"Ensuring output directory '{directory_name}' exists...")
    os.makedirs(directory_name, exist_ok=True)
    print(f"Output will be saved in: {os.path.abspath(directory_name)}")


# --- Step 2: Read Table Names ---
def read_table_names(file_path: str) -> list[str]:
    """
    Reads table names from a text file, one name per line.

    Args:
        file_path (str): Path to the text file containing table names.

    Returns:
        list[str]: A list of table names. Returns an empty list if an error occurs.
    """
    table_names = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                table_name = line.strip()
                if table_name:  # Ensure the line is not empty
                    table_names.append(table_name)
        print(f"Successfully loaded {len(table_names)} table names from '{file_path}'.")
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found. Please ensure it exists.")
    except Exception as e:
        print(f"An unexpected error occurred while reading the input file: {e}")
    return table_names


# --- Step 3: Process Each Table ---
def process_table(table_name: str, base_url: str, output_dir: str, sleep_time: int):
    """
    Fetches the metadata page for a single table, extracts column names,
    and creates a CSV file with these headers.

    Args:
        table_name (str): The name of the table to process.
        base_url (str): The base URL for EPA Envirofacts metadata.
        output_dir (str): The directory where CSV files will be saved.
        sleep_time (int): Time in seconds to pause after the request.
    """
    url = base_url + table_name.lower()  # EPA URLs often use lowercase for table names

    try:
        # Fetch the web page
        print(f"Attempting to fetch URL: {url}")
        response = requests.get(url, timeout=10)  # Add a timeout for robustness
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the target div
        # The data-v- attribute might change, so we look for a div that starts with 'data-v-'
        # and potentially contains specific content if the exact attribute changes
        target_div = soup.find('div', attrs={'data-v-ca6f7e46': True})

        if not target_div:
            print(
                f"Warning: Could not find the expected div (data-v-ca6f7e46) for table '{table_name}'. Skipping.")
            return  # Exit this function for the current table

        # Find anchor tags within the target div
        column_name_elements = target_div.find_all('a')

        column_names = []
        if column_name_elements:
            for element in column_name_elements:
                name = element.get_text(strip=True)
                print(f"\tFound column name '{name}' for table '{table_name}'.")
                if name:
                    column_names.append(name)
        else:
            print(f"Warning: No column name links found within the target div for table '{table_name}'.")

        if not column_names:
            print(f"No column names extracted for table '{table_name}'. Creating an empty CSV file.")

        # Write the column names to a CSV file
        csv_filepath = os.path.join(output_dir, f"{table_name}.csv")
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            if column_names:
                csv_writer.writerow(column_names)

        print(f"Successfully created '{csv_filepath}' with {len(column_names)} columns.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for '{table_name}': {http_err} (Status Code: {response.status_code})")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred for '{table_name}': {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred for '{table_name}': {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected request error occurred for '{table_name}': {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred while processing '{table_name}': {e}")
    finally:
        # Rate limiting
        print(f"Waiting {sleep_time} second(s) before next request...")
        time.sleep(sleep_time)


# --- Main Function ---
def main():
    """
    Main function to orchestrate the column extraction process.
    """
    # Step 1: Create output directory
    create_output_directory(OUTPUT_DIR)

    # Step 2: Read table names from the input file
    table_names = read_table_names(INPUT_FILE_NAME)

    if not table_names:
        print("No table names to process. Exiting.")
        return

    # Step 3: Process each table name
    for i, table_name in enumerate(table_names):
        print(f"\n--- Processing table {i + 1}/{len(table_names)}: '{table_name}' ---")
        process_table(table_name, BASE_URL, OUTPUT_DIR, SLEEP_TIME)

    print("\n--- Processing complete ---")
    print(f"CSV files saved in the '{OUTPUT_DIR}' directory.")


if __name__ == "__main__":
    main()
