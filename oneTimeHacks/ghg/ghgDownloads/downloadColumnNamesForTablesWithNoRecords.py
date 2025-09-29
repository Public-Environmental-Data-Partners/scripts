# downloadColumnNamesForTablesWithNoRecords.py
# This script was used to generate header-only CSV files for the 14 GHG tables
# that were recorded as having 0 records and that did not generate output
# CSV files when the dmap-graphql-api download code as run. This code uses
# a hand rolled .txt file to drive the process, then accesses an envirofacts
# endpoint to harvest the column names.
# This code uses the "model" pages on the GHG website that are believed to
# be somewhat out of date. So, consider it a best efforts attempt to
# generate useful data but the output has not been verified or used in
# any downstream processes.

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import os
import time
import csv

# --- Configuration ---
# IMPORTANT: You need to have a WebDriver (e.g., ChromeDriver) installed and its path
# accessible to your system or specified here.
# Download ChromeDriver matching your Chrome browser version from:
# https://chromedriver.chromium.org/downloads
# You might need to place it in a directory that's in your system's PATH,
# or provide the full path to the executable here.
# Example: CHROME_DRIVER_PATH = "/usr/local/bin/chromedriver"
# If ChromeDriver is in your PATH, you can leave this as None.
CHROME_DRIVER_PATH = None  # Set this to your ChromeDriver path if not in PATH

# Base URL for the EPA Envirofacts metadata pages
BASE_METADATA_URL = "https://enviro.epa.gov/envirofacts/metadata/table/ghg/"

# Output directory for CSV files
OUTPUT_DIR = "download_empties"

# Time to wait for elements to appear (in seconds)
WAIT_TIMEOUT = 20

# Time to pause between requests (in seconds)
SLEEP_TIME = 1

# Input file that holds the names of the empty tables
INPUT_FILE_NAME = "emptyTableNames.txt"

LOG_FILENAME = "emptyTableColumns.log"

# --- Global Logger (for demonstration, can be replaced with your LOG object) ---
import sys

try:
    LOG = LOG  # Use existing global LOG if it exists
except NameError:
    LOG = sys.stdout  # Otherwise, default to printing to console


# --- Step 1: Create Output Directory ---
def create_output_directory(directory_name: str):
    """
    Creates the specified output directory if it does not already exist.

    Args:
        directory_name (str): The name of the directory to create.
    """
    print(f"Ensuring output directory '{directory_name}' exists...", file=LOG)
    os.makedirs(directory_name, exist_ok=True)
    print(f"Output will be saved in: {os.path.abspath(directory_name)}", file=LOG)


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
        print(f"Successfully loaded {len(table_names)} table names from '{file_path}'.", file=LOG)
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found. Please ensure it exists.", file=LOG)
    except Exception as e:
        print(f"An unexpected error occurred while reading the input file: {e}", file=LOG)
    return table_names


# --- Helper function to initialize Selenium WebDriver ---
def _initialize_webdriver():
    """
    Initializes and returns a headless Chrome WebDriver.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode (no UI)
    chrome_options.add_argument("--disable-gpu")  # Recommended for headless
    chrome_options.add_argument("--no-sandbox")  # Required for some environments
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcomes limited resource problems

    try:
        if CHROME_DRIVER_PATH:
            service = Service(executable_path=CHROME_DRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            # Assumes chromedriver is in PATH
            driver = webdriver.Chrome(options=chrome_options)
        print("WebDriver initialized successfully.", file=LOG)
        return driver
    except WebDriverException as e:
        print(f"Error initializing WebDriver: {e}", file=LOG)
        print("Please ensure ChromeDriver is installed and its path is correctly set or in your system's PATH.",
              file=LOG)
        return None


# --- Step 3: Extract Column Names using Selenium ---
def extract_column_names_selenium(driver: webdriver.Chrome, table_name: str, base_url: str, wait_timeout: int) -> list[
                                                                                                                      str] | None:
    """
    Navigates to the table's metadata page, waits for dynamic content,
    and extracts column names using Selenium.

    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.
        table_name (str): The name of the table (e.g., "aa_tier1equationinput").
        base_url (str): The base URL for EPA Envirofacts metadata.
        wait_timeout (int): Maximum time to wait for elements to appear.

    Returns:
        list[str] | None: A list of extracted column names, or None if extraction fails.
    """
    url = base_url + table_name.lower()
    print(f"  Navigating to URL: {url}", file=LOG)

    try:
        driver.get(url)
        #print("waiting for main content area to load", file=LOG)
        # Wait for the main content area to load.
        # Based on inspection, the content seems to load into the #app div.
        # A good indicator that the content is loaded might be the presence of an <h3> tag
        # with "Columns" text, or the first anchor tag within the column list.

        # Let's try waiting for the 'Columns' heading or a specific element that contains the column links.
        # The structure is often: #app -> div -> h2 (Columns) -> div -> a (column names)
        # We'll wait for the presence of the first column name link as a reliable indicator.

        # A robust XPath to find the column name links:
        # //div[@id='app']//h2[contains(text(), 'Columns')]/following-sibling::div[1]//a
        # This looks for an <h2> with "Columns" text inside #app, then its immediate following sibling div,
        # and then any <a> tags within that div.

        # Wait for at least one column link to be present
        column_link_xpath = "//div[@id='app']//h3[contains(text(), 'Columns')]/following-sibling::div[1]//a"
        WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_element_located((By.XPATH, column_link_xpath))
        )
        print(f"  Page content loaded for {table_name}.", file=LOG)

        # Find all column name links
        column_elements = driver.find_elements(By.XPATH, column_link_xpath)

        column_names = []
        for element in column_elements:
            name = element.text.strip()
            if name:
                column_names.append(name)

        if not column_names:
            print(f"  Warning: No column names extracted from links for {table_name}.", file=LOG)
            return None  # Indicate failure to extract columns

        print(f"  Extracted {len(column_names)} column names for {table_name}.", file=LOG)
        return column_names

    except TimeoutException:
        print(f"  Timeout: Content did not load within {wait_timeout} seconds for {table_name}.", file=LOG)
        return None
    except NoSuchElementException:
        print(f"  Could not find expected column elements for {table_name}. HTML structure might have changed.",
              file=LOG)
        print(f"  Current page source (first 1000 chars): {driver.page_source[:1000]}...", file=LOG)
        return None
    except WebDriverException as e:
        print(f"  WebDriver error for {table_name}: {e}", file=LOG)
        return None
    except Exception as e:
        print(f"  An unexpected error occurred during Selenium extraction for {table_name}: {e}", file=LOG)
        return None


# --- Step 4: Write Column Names to CSV ---
def write_column_names_to_csv(table_name: str, column_names: list[str], output_dir: str):
    """
    Writes the extracted column names to a CSV file.

    Args:
        table_name (str): The name of the table.
        column_names (list[str]): A list of column names to write.
        output_dir (str): The directory where the CSV file will be saved.
    """
    csv_filepath = os.path.join(output_dir, f"{table_name}.csv")
    try:
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            if column_names:
                csv_writer.writerow(column_names)
            else:
                # If column_names is empty, an empty file will be created.
                # This fulfills the requirement of having a file for each table.
                print(f"  No column names provided for '{table_name}'. Creating an empty CSV file.", file=LOG)

        print(f"  CSV file '{csv_filepath}' created with {len(column_names)} columns.", file=LOG)
    except Exception as e:
        print(f"  Error writing CSV file for {table_name}: {e}", file=LOG)


# --- Main Function ---
def main():
    """
    Main function to orchestrate the column extraction process using Selenium.
    """
    # Step 0: open log file
    global LOG
    try:
        LOG = open(LOG_FILENAME, "w", encoding="utf-8")
        print(f"Log file '{LOG_FILENAME}' opened successfully.", file=LOG)
    except IOError as e:
        print(f"Error opening log file: {e}")
        LOG = None
        return # Exit if we can't open the log file

    # Step 1: Create output directory
    create_output_directory(OUTPUT_DIR)

    # Step 2: Read table names from the input file
    table_names = read_table_names(INPUT_FILE_NAME)

    if not table_names:
        print("No table names to process. Exiting.", file=LOG)
        return

    # Initialize WebDriver once for all tables
    driver = _initialize_webdriver()
    if not driver:
        print("Failed to initialize WebDriver. Exiting.", file=LOG)
        return

    try:
        # Step 3 & 4: Process each table name, extract columns, and write CSV
        for i, table_name in enumerate(table_names):
            print(f"\n--- Processing table {i + 1}: '{table_name}' ---", file=LOG, flush=True)

            # Extract column names using Selenium
            extracted_columns = extract_column_names_selenium(driver, table_name, BASE_METADATA_URL, WAIT_TIMEOUT)

            # Write to CSV (even if extracted_columns is None or empty)
            write_column_names_to_csv(table_name, extracted_columns if extracted_columns is not None else [],
                                      OUTPUT_DIR)

            # Rate limiting
            print(f"Waiting {SLEEP_TIME} second(s) before next request...", file=LOG)
            time.sleep(SLEEP_TIME)

    finally:
        # Ensure the browser is closed even if errors occur
        if driver:
            driver.quit()
            print("\nWebDriver closed.", file=LOG)

    print("\n--- Processing complete ---", file=LOG)
    print(f"CSV files saved in the '{OUTPUT_DIR}' directory.", file=LOG)


if __name__ == "__main__":
    main()
