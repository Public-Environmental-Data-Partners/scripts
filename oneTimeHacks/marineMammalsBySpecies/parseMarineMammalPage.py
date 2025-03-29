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

# TODO: need to figure out how to account for and deal with very long file paths
# generated by file-izing the longer text headers.
# There is said to be a 260 char limit for file paths and a limit of about
# 255 chars for filenames. Two of the marine mammal file paths exceeded this limit
# and had to be downloaded manually with shortened filenames which is less than ideal.
# It would be better to create shorter folder names although what a generalized
# algorithm would be for that I do not know.

def text2validFileFolderName(text):
    """Converts a string to snake_case suitable for use as a folder or file name"""
    text = re.sub(r'\s*\(.*?\)', '', text)  # Remove space and parentheses
    text = re.sub(r'[^\x00-\x7F]+', '', text) # Remove non-ASCII characters
    text = re.sub(r'[.,;:"/\']', '', text)  # Remove unwanted chars
    text = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', text)
    text = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', text)
    text = text.replace(" ", "_").replace("-", "_") # replace spaces with underscores
    text = text.replace("-", "_")  # replace hyphens with underscores
    text = re.sub(r'_+', '_', text)  # Remove multiple underscores
    return text.lower()

def getReportsByGroup(h2, groupList, log):
    groupName = h2.text.strip()
    print("in getReportsByGroup for group:", groupName, file=log)
    groupDict = {
        "group" : groupName,
        "groupFolder" : text2validFileFolderName(groupName),
        "speciesDict" : {}
    }
    next_sibling = h2.find_next_sibling()
    print("starting species:", next_sibling, file=log)
    h3List = []
    while next_sibling:
        if isinstance(next_sibling, Tag):
            if next_sibling.name == "h2":
                print("found next h2", file=log)
                break
            if next_sibling.name == "h3":
                #print(f"H3: {next_sibling.text}", file=log)
                print("found species:", next_sibling, file=log)
                h3List.append(next_sibling)
                speciesName = next_sibling.text.strip()
                #print("group name", groupDict["group"], "species name:", speciesName, file=log)
                groupDict["speciesDict"][speciesName] = {}
                speciesFolder = text2validFileFolderName(speciesName)
                groupDict["speciesDict"][speciesName]["speciesFolder"] = speciesFolder
                groupDict["speciesDict"][speciesName]["regionList"] = []
                ul_tag = next_sibling.find_next_sibling("ul")
                if ul_tag:
                    li_tags = ul_tag.find_all("li")
                    for li in li_tags:
                        strong_tag = li.find("strong")
                        if strong_tag:
                            #print(f"  Strong: {strong_tag.text}", file=log)
                            regionName = strong_tag.text.strip()
                            region = {
                                "region": regionName,
                                "regionFolder": text2validFileFolderName(regionName),
                                "fileList": []
                            }
                            print("region name:", regionName, file=log)
                            a_tags = li.find_all("a")
                            for a in a_tags:
                                #print(f"    Href: {a['href']}", file=log)
                                region["fileList"].append({"text": a.text.strip(), "href": a["href"]})
                            groupDict["speciesDict"][speciesName]["regionList"].append(region)

        next_sibling = next_sibling.find_next_sibling()
    groupList.append(groupDict)
    return
    #print(json.dumps(groupList, indent=2, ensure_ascii=False), file=log)


def print_data(groupList, log):
    for group_data in groupList:
        group_folder = group_data["groupFolder"]
        print(group_folder, file=log)
        for species, species_data in group_data["speciesDict"].items():
            species_folder = species_data["speciesFolder"]
            print("\t", species_folder, file=log)
            if len(species_data["regionList"]) > 0:
                for region_data in species_data["regionList"]:
                    region_folder = region_data["regionFolder"]
                    print("\t\t", region_folder, file=log)
                    for file_data in region_data["fileList"]:
                        href = file_data["href"]
                        print("\t\t\t", href, file=log)
            else:
                print("\t\tno regionList", file=log)

def main():
    log = open("parse.log", "w", encoding="utf-8")
    print("in main, about to get page and instantiate soup", file=log)
    url = "https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-species-stock"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.content, "html.parser")
    except FileNotFoundError:
        print(f"Error: Page not found at {url}")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(2)
    print("back from file open and soup instantiation", file=log)
    print("page title is: ", soup.title, file=log)
    h2List = soup.find_all("h2")
    groupList = []
    for h2 in h2List:
        h2Title = h2.text.strip()
        print(f"h2Title: {h2Title}", file=log)
        if (h2Title.startswith("Marine Mammals")):
            print(f"not processing: |{h2Title}|", file=log)
        elif (h2Title != "On This Page") and (h2Title != "More Information"):
            getReportsByGroup(h2, groupList, log)
            print("back from getReportsByYear, groupList count:", len(groupList), file=log)
        else:
            print(f"not processing: |{h2Title}|", file=log)

    downloadDict = {
        "downloadFolder": "download",
        "groupList" : groupList
    }

    dictFilename = "downloadDict.json";
    try:
        with open(dictFilename, "w", encoding="utf-8") as jsonFile:
            json.dump(downloadDict, jsonFile, indent=2, ensure_ascii=False)
            print("dictionary dumped to:", dictFilename)
    except OSError as e:
        print(f"Error writing to file: {e}")

main()