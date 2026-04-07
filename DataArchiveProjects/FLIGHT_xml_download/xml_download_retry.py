# This script is a modest variation on xml_download.py
# So see that file for a general explanation.
# The difference in this file is that, rather than use one
# of the flight csv files as input, it uses as input the
# failure.csv file of the original script and, for any files
# listed there that didn't generate a simple 500 error code,
# it retries the download. This did garner a few more xml files
# the first time I ran it. But then no more.
#
# ~ Anne Gunn, Sept 2025

import os
import csv
from datetime import datetime
import random
import requests

# --- Global Constants ---
# The main input file driving the retry process.
# Expected columns: FACILITY_ID, YEAR, STATE, ERROR_CODE
INPUT_CSV = './failure.csv'

# Directory where all downloaded files will be stored, organized by state.
DOWNLOAD_DIR = 'download'

# Log files for tracking script execution and outcomes.
PROCESS_LOG_FILE_NAME = 'retry.log'
SUCCESS_LOG_FILE_NAME = 'success.csv'
FAILURE_LOG_FILE_NAME = 'retry_failure.csv'
PROCESS_LOG = None
SUCCESS_LOG = None
FAILURE_LOG = None

def setupLogging():
    """Set up the various logging/tracking files."""
    global PROCESS_LOG
    PROCESS_LOG = open(PROCESS_LOG_FILE_NAME, "w", encoding="utf-8")
    print(f"Process log opened successfully at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", file=PROCESS_LOG)
    global SUCCESS_LOG
    # Each file is only downloaded once, skipped any other time the code is run, so
    # we want to append to this log.
    SUCCESS_LOG = open(SUCCESS_LOG_FILE_NAME, "a", encoding="utf-8")
    print(f"Success log re-opened successfully at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", file=PROCESS_LOG)
    global FAILURE_LOG
    # all the failures get retried each run. So rather than appending to this log, we overwrite it.
    FAILURE_LOG = open(FAILURE_LOG_FILE_NAME, "w", encoding="utf-8")
    print(f"Failure log re-opened successfully at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", file=PROCESS_LOG)

def shutdownLogging():
    """Closes all logging files if they are open."""
    if PROCESS_LOG:
        PROCESS_LOG.close()
    if SUCCESS_LOG:
        SUCCESS_LOG.close()
    if FAILURE_LOG:
        FAILURE_LOG.close()

def ensureDownloadDirExists(state):
    """
    Tests to see if the download directory for a given state exists;
    if not, creates it.
    e.g., ./download/TX/
    """
    state_dir = os.path.join(DOWNLOAD_DIR, state)
    if not os.path.exists(state_dir):
        os.makedirs(state_dir)
        print(f"Created directory: {state_dir}", file=PROCESS_LOG)
    #else:
    #    print(f"Directory already exists: {state_dir}", file=PROCESS_LOG)


def processRow(facility_id, year, state):
    """
    Handles the logic for a single row from the input CSV.
    - Constructs URL and file path
    - Tests to see if the file already exists locally
    - If not, attempts to download it via requests.
    - Logs success or failure.
    """
    ensureDownloadDirExists(state)
    url = f"https://ghgdata.epa.gov/ghgp/service/xml/{year}?id={facility_id}&et=undefined"
    file_path = os.path.join(DOWNLOAD_DIR, state, f"{facility_id}_{year}.xml")
    #print(f"URL: {url}", file=PROCESS_LOG)
    #print(f"File path: {file_path}", file=PROCESS_LOG)
    if os.path.exists(file_path):
        print(f"File already exists, skipping download: {file_path}", file=PROCESS_LOG)
        return 'skipped'
    else:
        status_tuple = downloadFile(url, file_path)
        if status_tuple[0]:
            logSuccess(facility_id, year, state, file_path, url)
            return 'success'
        else:
            error_message = status_tuple[1] if status_tuple[1] else "Unknown error"
            logFailure(facility_id, year, state, error_message, url)
            return 'failure'


def downloadFile(url, destination_path):
    """Attempts to download a file from a given URL using requests."""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(destination_path, 'wb') as f:
                f.write(response.content)
            return True, None
        else:
            return False, f"HTTP {response.status_code}: {response.reason}"
    except Exception as e:
        return False, str(e)


def logSuccess(facility_id, year, state, file_path, url=None):
    """Appends a record to the success CSV log, including the URL."""
    if SUCCESS_LOG:
        SUCCESS_LOG.write(f"{facility_id},{year},{state},{file_path},{url}\n")
        SUCCESS_LOG.flush()
    print(f"SUCCESS: {facility_id}, {year}, {state}, {file_path}, {url}", file=PROCESS_LOG)


def logFailure(facility_id, year, state, error_message, url=None):
    """Appends a record to the failure CSV log, including the URL."""
    if FAILURE_LOG:
        FAILURE_LOG.write(f"{facility_id},{year},{state},\"{error_message}\",{url}\n")
        FAILURE_LOG.flush()
    print(f"FAILURE: {facility_id}, {year}, {state}, {error_message}, {url}", file=PROCESS_LOG)


def main():
    """
    Main function to drive the download process.
    - Sets up logging.
    - Reads the input CSV.
    - Calls processRow for each row in the CSV.
    """
    setupLogging()
    start_time = datetime.now()
    print(f"Starting EPA XML file download process at {start_time.strftime('%Y-%m-%d %H:%M:%S')}", file=PROCESS_LOG)

    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    try:
        with open(INPUT_CSV, mode='r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for idx, row in enumerate(reader, start=1):
                # Use this to limit the number of rows downloaded in any single run.
                # The code is built to be restartable, so you can run it multiple times
                # if downloaded_count > 75000: break
                # *********************************
                if not all(key in row for key in ['FACILITY_ID', 'YEAR', 'STATE', 'ERROR_CODE']):
                    print(f"***Skipping row {idx} with missing data: {row}", file=PROCESS_LOG)
                    continue
                facility_id = row['FACILITY_ID']
                year = row['YEAR']
                state = row['STATE']
                error = row['ERROR_CODE'                ]
                if error.startswith('HTTP 500:'):
                    print(
                        f"Skipping HTTP 500 row {idx}: Facility ID: {facility_id}, Year: {year}, State: {state}, Error Code: {error}",
                        file=PROCESS_LOG)
                    continue
                print(f"Processing row {idx}: Facility ID: {facility_id}, Year: {year}, State: {state}, Error Code: {error}", file=PROCESS_LOG)
                # Call processRow and get status
                status = processRow(facility_id, year, state)
                if status == 'skipped':
                    skipped_count += 1
                elif status == 'success':
                    downloaded_count += 1
                elif status == 'failure':
                    failed_count += 1
                else:
                    print(f"***Unexpected status '{status}' for row {idx}", file=PROCESS_LOG)
                sleep_time = random.uniform(0.1, 0.5)
                if (idx % 100) == 0:
                    print(f"Processed {idx} rows so far... Skipped: {skipped_count}, Downloaded: {downloaded_count}, Failed: {failed_count}")
    except FileNotFoundError:
        print(f"Error: The input file '{INPUT_CSV}' was not found.", file=PROCESS_LOG)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=PROCESS_LOG)
    end_time = datetime.now()
    elapsed = end_time - start_time
    print(f"...Process complete at {end_time.strftime('%Y-%m-%d %H:%M:%S')}", file=PROCESS_LOG)
    print(f"Elapsed time: {str(elapsed)}", file=PROCESS_LOG)
    print(f"Total files skipped (already downloaded): {skipped_count}", file=PROCESS_LOG)
    print(f"Total files downloaded: {downloaded_count}", file=PROCESS_LOG)
    print(f"Total failures: {failed_count}", file=PROCESS_LOG)
    print(f"...Process complete at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Elapsed time: {str(elapsed)}")
    print(f"Total files skipped (already downloaded): {skipped_count}")
    print(f"Total files downloaded: {downloaded_count}")
    print(f"Total failures: {failed_count}")
    shutdownLogging()


if __name__ == "__main__":
    main()
