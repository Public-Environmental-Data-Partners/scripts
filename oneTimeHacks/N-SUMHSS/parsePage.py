# Code to get data from:
# https://www.samhsa.gov/data/data-we-collect/n-sumhss-national-substance-use-and-mental-health-services-survey/datafiles
# To be compatible with the getFiles.py code, the code needs to output a json structure like the one
# in sampleJsonDict.json in this folder.
# The dictionary must have a series of nested folder dictionaries where, for each folder, there are the following
# elements:
#  folder -- a valid file folder name
#  subfolderList -- a list of dictionaries like this one for each subfolder, may be empty
#  downloadList -- a list of absolute URLs to files to be downloaded, may be empty
# In addition, the dictionary entries can have any working data values of use to the coder. All will be ignored.

from bs4 import BeautifulSoup, Tag
import json
import os
from pathlib import Path
import re
import requests
import time
import urllib.parse

def getRawYearList(soup, log):
    year_data_list = []
    survey_year_container = soup.find("div", {"id": "surveyYearSelectorContainer"})
    if survey_year_container:
        li_elements = survey_year_container.find_all("li")
        for li in li_elements:
            a_tag = li.find("a")
            if a_tag and "href" in a_tag.attrs and a_tag.text.strip().isdigit():
                href = a_tag["href"]
                year = a_tag.text.strip()
                collection_list = []
                if "data-all-data-collections" in a_tag.attrs:
                    collection_str = a_tag["data-all-data-collections"]
                    collection_list = [c.strip() for c in collection_str.split(",")]
                else:
                    print("*** error -- could not find collection string", file=log)

                if year is not None:
                    year_data = {
                        "year": year,
                        "original_href": href,
                        "collectionList": collection_list
                    }
                    year_data_list.append(year_data)
                    #print("year:", year, "original_href:", href, file=log)
    return year_data_list

# On inspection and manual testing of various URL combinations, it appears that
# the right Dataset Download links always appear if one simply appends
# the year and the collection number to the end of the base URL.
# That is, taking: https://www.samhsa.gov/data/data-we-collect/n-sumhss-national-substance-use-and-mental-health-services-survey/datafiles?
# and adding to the end
# year=yyyy&data_collection=nnnn
# will get you to a page that has the right download links
baseURL = "https://www.samhsa.gov/data/data-we-collect/n-sumhss-national-substance-use-and-mental-health-services-survey/datafiles?"
def getCollectionPageLists(yearList, log):
    print("in getCollectionPageLists", file=log)
    for entry in yearList:
        entry["folder"] = "year_" + entry["year"]
        entry["collectionInfoList"] = []
        for collectionNum in entry["collectionList"]:
            dataCollectionURL = f"{baseURL}year={entry['year']}&data_collection={collectionNum}"
            collectionInfo = {
                "collectionNum": collectionNum,
                "dataCollectionURL": dataCollectionURL
            }
            entry["collectionInfoList"].append(collectionInfo)
        json.dump(entry, log, indent=2, ensure_ascii=False)

    return yearList

def getCollectionTagFromURL(relativeURL, log):
    collectionTag = None
    match = re.search(r"puf-file/([A-Z-]+)-", relativeURL)
    if match:
        collectionTag = match.group(1)
    print("relative URL:", relativeURL, "collectionTag:", collectionTag, file=log)
    return collectionTag

def getDownloadLinkLists(yearList, log):
    print("in getDownloadLinkLists", file=log)
    for entry in yearList:
        entry["subfolderList"] = []
        for collection in entry["collectionInfoList"]:
            # intentionally to-console
            print("getting download links for year/collection", entry["year"], collection["collectionNum"])
            folder = {
                "folder": "collection_"+collection["collectionNum"],
            }
            downloadLinkList = []
            # get the collection page from its url
            collectionSoup = None
            try:
                response = requests.get(collection["dataCollectionURL"])
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                response.encoding = 'utf-8'
                collectionSoup = BeautifulSoup(response.content, "html.parser")
                #print("have collection page!", file=log)
            except FileNotFoundError:
                print(f"***Error: Page not found at {collection["dataCollectionURL"]}", file=log)
                continue
            except Exception as e:
                print(f"***An error occurred: {e}", file=log)
                continue

            if collectionSoup is not None:
                # pull out the links to the docs
                documentation_heading = collectionSoup.find("h3", class_="puf__label", string="Dataset Documentation")
                if documentation_heading:
                    documentation_div = documentation_heading.find_next_sibling("div",
                                                                                class_="display-flex flex-column gap-105")
                    if documentation_div:
                        for a_tag in documentation_div.find_all("a", href=True):
                            relativeURL = a_tag["href"]
                            downLoadLink = "https://www.samhsa.gov"+ relativeURL
                            downloadLinkList.append(downLoadLink)
                            if "info-codebook" in relativeURL:
                                # Just a hack but this looks a) useful and b) like it will work
                                # Use this specific link to pull out a more meaningful tag for the
                                # collection type (not just a number) and add it to folder name
                                collectionTag = getCollectionTagFromURL(relativeURL, log)
                                if collectionTag is not None:
                                    folder["folder"] = folder["folder"] + "-" + collectionTag
                            else:
                                # There's always got to be one problem child
                                if folder["folder"] == "collection_1282":
                                    folder["folder"] = folder["folder"] + "-UFDS"
                else:
                    print("no documentation heading", file=log)
                # pull out the links to the data files themselves
                downloads_heading  = collectionSoup.find("h3", class_="puf__label", string="Dataset Downloads")
                if downloads_heading :
                    downloads_div = downloads_heading.find_next_sibling("div",
                                                                                class_="grid-row")
                    if downloads_div:
                        for a_tag in downloads_div.find_all("a", href=True):
                            relativeURL = a_tag["href"]
                            downLoadLink = "https://www.samhsa.gov" + relativeURL
                            downloadLinkList.append(downLoadLink)
                else:
                    print("no downloads heading", file=log)
                # if there are setup files, grab them also
                setups_heading = collectionSoup.find("h3", class_="puf__label", string="ASCII Setup Files")
                if setups_heading:
                    print("found setup files div", file=log)
                    setups_div = setups_heading.find_next_sibling("div",
                                                                        class_="display-flex")
                    if setups_div:
                        for a_tag in setups_div.find_all("a", href=True):
                            relativeURL = a_tag["href"]
                            downLoadLink = "https://www.samhsa.gov" + relativeURL
                            downloadLinkList.append(downLoadLink)
                            print("added setup file:", downLoadLink, file=log)
                else:
                    print("no setups heading", file=log)

            folder["downloadList"] = downloadLinkList
            entry["subfolderList"].append(folder)
    return yearList

def countFiles(dict, log):
    totalDownloads = 0

    for year in dict["subfolderList"]:
        for collection in year["subfolderList"]:
            totalDownloads += len(collection["downloadList"])

    print("have", totalDownloads, "URLs to download", file=log)
    return totalDownloads

def main():
    log = open("parse.log", "w", encoding="utf-8")
    print("in main, about to get page and instantiate soup", file=log)
    url = "https://www.samhsa.gov/data/data-we-collect/n-sumhss-national-substance-use-and-mental-health-services-survey/datafiles"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.content, "html.parser")
        # use the code below to avoid beating the server during testing
        #sampleFile = open("firstPage.html", "r", encoding="utf-8")
        #soup = BeautifulSoup(sampleFile, "html.parser")
    except FileNotFoundError:
        print(f"Error: Page not found at {url}")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(2)
    print("back from file open and soup instantiation", file=log)
    print("page title is: ", soup.title, file=log)

    # the following print statements are intentionally to-console rather than to-log
    # they'll act as a sort of keep-alive notice for potentially long-running steps
    rawYearList = getRawYearList(soup, log)
    print("number raw year list entries: ", len(rawYearList))
    yearList = getCollectionPageLists(rawYearList, log)
    print("back from getCollectionPageLists")
    yearList = getDownloadLinkLists(yearList, log)
    print("back from getDownloadLinkLists")
    #json.dump(yearList, log, indent=2, ensure_ascii=False)

    downloadDict = {
        "folder": "download",
        "folderLevel": 0,
        "subfolderList" : yearList,
        "downloadList": []  # don't expect any but let's be consistent
    }

    countFiles(downloadDict, log)

    dictFilename = "downloadDict.json"
    try:
        with open(dictFilename, "w", encoding="utf-8") as jsonFile:
            json.dump(downloadDict, jsonFile, indent=2, ensure_ascii=False)
            print("dictionary dumped to:", dictFilename)
    except OSError as e:
        print(f"Error writing to file: {e}")

main()