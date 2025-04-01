# Code to get data from:
# https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-species-stock

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

def getFiles(groupList, log):
    stats = { "downloadCount" : 0, "errorCount" : 0, "skipCount" : 0 }
    downloadLimit = 0  # use to force partial runs, set to 0 for a full run
    for group_data in groupList:
        group_folder = group_data["groupFolder"]
        print("*** new group ****: ", group_folder, file=log)
        makeAndChangeToFolder(group_folder, log)
        for species, species_data in group_data["speciesDict"].items():
            species_folder = species_data["speciesFolder"]
            print("\t", species_folder, file=log)
            if len(species_folder) > 0 and len(species_data["regionList"]) > 0:
                makeAndChangeToFolder(species_folder, log)
                for region_data in species_data["regionList"]:
                    region_folder = region_data["regionFolder"]
                    print("\t\t", region_folder, file=log)

                    # Risso's dolphin section has something weird in the HTML
                    # that is creating a duplicate file list with a blank region
                    # I'm only finding one of these so, for now, punt and skip it
                    if (region_folder != "" and
                            (downloadLimit == 0 or (downloadLimit > 0 and stats["downloadCount"] < downloadLimit))):
                        makeAndChangeToFolder(region_folder, log)
                        for file_data in region_data["fileList"]:
                             href = file_data["href"]
                             print("\t\t\t", href, file=log)
                             getOneFile(href, stats, log)
                        os.chdir("..") # exit region folder
                        #print("step up cwd:", os.getcwd(), file=log)
                    else:
                        print("skipped region", file=log)
                os.chdir("..")  # exit species folder
                #print("step up cwd:", os.getcwd(), file=log)
            else:
                 print("\t\tno regionList", file=log)
        os.chdir("..") #exit group folder
        #print("step up cwd:", os.getcwd(), file=log)
    os.chdir("..") # exit download folder
    #print("step up cwd:", os.getcwd(), file=log)
    print(json.dumps(stats, indent=4), file=log)

def main():
    log = open("get.log", "w", encoding="utf-8")
    print("in main, about to open json file", file=log)

    jsonPath = "downloadDict.json"

    try:
        with open(jsonPath, "r") as json_file:
            downladDict = json.load(json_file)
            print(f"Dictionary loaded from {jsonPath}")
    except OSError as e:
        print(f"Error reading from file: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding json: {e}")

    # TODO: this should come from the json file but for now, hardwire
    # create a master folder to hold all the downloads
    makeAndChangeToFolder("download", log)

    getFiles(downladDict["groupList"], log)


main()