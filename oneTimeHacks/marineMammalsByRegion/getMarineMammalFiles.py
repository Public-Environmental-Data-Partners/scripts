# This code was written for and tested against the files from page:
# https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-region
# It's been somewhat generalized from a previous version to make it easier to adapt for other
# datasets. Specifically, numeric folder levels are assigned in the data structure so we know
# when to cd .. back up to a parent level

from bs4 import BeautifulSoup, Tag
import json
import os
from pathlib import Path
import re
import requests
import time
import urllib.parse



def makeAndChangeToFolder(folderName, log):
    if not os.path.exists(folderName):
        os.makedirs(folderName)
    os.chdir(folderName)
    #print("step down cwd:", os.getcwd(), file=log)

def getOneFile(downloadURL, stats, log):
    parsed_url = urllib.parse.urlparse(downloadURL)
    path = parsed_url.path
    filename = os.path.basename(path)
    if os.path.exists(filename):
        print("skipping", filename, "already exists", file=log)
        stats["skipCount"] += 1
    else:
        print("about to get: ", downloadURL, file=log)
        downloadOk = False
        try:
            response = requests.get(downloadURL)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            if response.status_code == 200:  # 200 means the file exists
                parsed_url = urllib.parse.urlparse(downloadURL)
                path = parsed_url.path
                filename = os.path.basename(path)

                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print("File written successfully to: ", filename, file=log)
                stats["downloadCount"] += 1
                downloadOk = True
                time.sleep(3)  # pause 3 second between files
                if stats["downloadCount"] % 10 == 0:
                    # don't write this to log file, want to see in the terminal
                    print("proof of life, download count is: ", stats["downloadCount"])
            elif response.status_code == 404:
                print("File not found.", file=log)
            else:
                print(f"Request failed with status code:", response.status_code, file=log)
        except requests.exceptions.MissingSchema as e:
            print(f"Error: Invalid URL - {e}", file=log)
        except requests.exceptions.RequestException as e:
            print(f"Error during download: {e}", file=log)
        except OSError as e:
            print(f"Error saving file: {e}", file=log)
        except Exception as e:  # Catch any other type of error.
            print(f"An unexpected error occurred: {e}", file=log)
        if downloadOk == False:
            stats["errorCount"] += 1

# Lordy, lordy a legitimate use for recursion!
def processNestedDictionary(nestedDict, stats, log):
    makeAndChangeToFolder(nestedDict["folder"], log)

    # Process downloadList
    for fileUrl in nestedDict.get("downloadList", []):
        getOneFile(fileUrl, stats, log)

    # Recurse into subfolders
    for subfolder in nestedDict.get("subfolderList", []):
        processNestedDictionary(subfolder, stats, log)

    os.chdir("..")  # Move back up after processing

def main():
    log = open("get.log", "w", encoding="utf-8")
    print("in main, about to open json file", file=log)

    jsonPath = "downloadDict.json"

    downloadDict = {}
    try:
        with open(jsonPath, "r") as json_file:
            downloadDict = json.load(json_file)
            print(f"Dictionary loaded from {jsonPath}", file=log)
    except OSError as e:
        print(f"Error reading from file: {e}", file=log)
    except json.JSONDecodeError as e:
        print(f"Error decoding json: {e}", file=log)

    stats = { "downloadCount" : 0, "errorCount" : 0, "skipCount" : 0 }
    processNestedDictionary(downloadDict, stats, log)
    print(json.dumps(stats, indent=4), file=log)

main()