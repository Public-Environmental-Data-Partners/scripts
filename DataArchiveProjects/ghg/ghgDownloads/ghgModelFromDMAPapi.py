# ghgModelFromDMAPapi.py
# This script interacts with the EPA DMAP API to retrieve metadata about
# greenhouse gas (GHG) data tables. It fetches a list of active GHG-related
# tables, then queries each table to obtain its record count. The results are
# written to a CSV file (`../ghgTableCount.csv`) containing table IDs, names,
# subject areas, and record counts. Logging is performed throughout the process
# to a log file (`ghgModel.log`) for tracking progress and errors.

import csv
import json # Often useful for handling JSON data
import requests
import time

# Global variable for the log file object used by most of the code
LOG = None
# Global variable for the api endpoint base URL
API_ENDPOINT = "https://data.epa.gov/dmapservice"

def get_table_list():
    table_list = []
    table_query = "metadata.qb_subject_area_tables/active/equals/true/join/metadata.qb_subject_areas/subject_area_id/equals/subject_area_id/program_code/equals/ghg/sort/title/base"
    url = API_ENDPOINT + "/" + table_query
    print("in get_table_list, about to request data from:", url, file=LOG)
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        # Check the response status code
        if response.status_code == 200:
            print("GET request successful! Status Code: 200 OK", file=LOG)

            # Parse the JSON response
            # response.json() automatically parses the JSON into a Python list/dictionary
            returned_data = response.json()
            # response should have two elements: data and status
            print("response has", len(returned_data), "elements", file=LOG)
            if returned_data["status"]:
                print("status code:", returned_data["status"], file=LOG)
            else:
                print("*** NO completion code:", file=LOG)
            if returned_data["data"] and returned_data["data"]["metadata__qb_subject_area_tables"]:
                print("got back info for", len(returned_data["data"]["metadata__qb_subject_area_tables"]), "tables", file=LOG)
            else:
                print("*** Did not get expected table data:", file=LOG)
            #print(json.dumps(returned_data, indent=2), file=LOG)
            table_list = returned_data["data"]["metadata__qb_subject_area_tables"]
        else:
            print("GET request failed! Status Code:", response.status_code, file=LOG)
    except FileNotFoundError:
        print(f"Error: Page not found at {url}")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(2)

    return table_list

def get_table_record_counts(table_list):
    print("in get_table_record_counts, about to request counts for:", len(table_list), "tables", file=LOG)
    table_count_list = []

    url = API_ENDPOINT + "/" + "query"
    print("post url:", url, file=LOG)
    for i, table in enumerate(table_list):
        if (i % 10 == 0):
            # intentionaly write this line to console as a heartbeat indicator
            print("about to process table number:", i)
        table_name = table["table_name"]
        query_table_name = "ghg__"+table_name
        graphql_query_string = f"""
        query aggregateCount {{
            {query_table_name} {{
                aggregate {{
                    count
                }}
            }}
        }}
        """
        payload = {"query": graphql_query_string}
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully retrieved data for {table_name}:", file=LOG)
            print(json.dumps(data, indent=2), file=LOG)
            record_count = data["data"][query_table_name][0]["count"]
        else:
            print(f"Error: {response.status_code} - {response.text}", file=LOG)
        simplified_table_data = {
            "table_id": table["table_id"],
            "table_name": table_name,
            "subject_area": table["subject_area"],
            "record_count": record_count
        }
        table_count_list.append(simplified_table_data)
        # be conservative and give the api a break between calls
        # yes, this will make the code take 6+ minutes to run
        time.sleep(1)

    print("returning counts for", len(table_count_list), "tables", file=LOG)
    return table_count_list

def main():
    global LOG
    LOG = open("ghgModel.log", "w", encoding="utf-8")
    print("in main, about to get table list", file=LOG)

    start_time = time.time() # Get the current time in seconds since the epoch
    print(f"API calls starting at: {time.ctime(start_time)}", file=LOG)
    print("-" * 30, file=LOG)

    table_list = get_table_list()
    print("in main, back from get table list", file=LOG)

    table_count_list = get_table_record_counts(table_list)

    csv_filename = "../ghgTableCount.csv"
    csv_file = open(csv_filename, "w", encoding="utf-8", newline='')
    fieldnames = table_count_list[0].keys()
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(table_count_list)
    print("done with csv output, see:", csv_filename, file=LOG)
    csv_file.close()

    end_time = time.time()  # Get the current time again
    print(f"script stopping at: {time.ctime(end_time)}", file=LOG)
    # Calculate the elapsed time
    elapsed_time = end_time - start_time
    print(f"Total elapsed time: {elapsed_time:.2f} seconds", file=LOG)

    LOG.close()

main()