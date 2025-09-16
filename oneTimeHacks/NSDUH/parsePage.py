# Code to parse a series of web pages starting from:
# https://www.samhsa.gov/data/data-we-collect/nsduh-national-survey-drug-use-and-health/datafiles
# To be compatible with the getFiles.py code, the code needs to output a json structure like the one
# into a download dictionary to be used by getFiles.py.
# The dictionary must have a series of nested folder dictionaries where, for each folder, there are the following
# elements:
#  folder -- a valid file folder name
#  subfolderList -- a list of dictionaries like this one for each subfolder, may be empty
#  downloadList -- a list of absolute URLs to files to be downloaded, may be empty
# In addition, the dictionary entries can have any working data values of use to the coder. All will be ignored.
from pydoc import pager

from bs4 import BeautifulSoup, Tag
import json
import os
from pathlib import Path
import re
import requests
import time
import urllib.parse

def extractYear(text, log):
    year = ""
    # Case 1: Year range (e.g., YYYY-YYYY)
    match_range = re.search(r"(\d{4}-\d{4})$", text)
    if match_range:
        year = match_range.group(1)
    else:
        # Case 2: Single year at the end
        match_single = re.search(r"(\d{4})$", text)
        if match_single:
            year = match_single.group(1)
        else:
            # Case 3: Year followed by " - Part A" or " - Part B"
            match_part = re.search(r"(\d{4})\s*-\s*Part\s*([AB])$", text, re.IGNORECASE)
            if match_part:
                year = match_part.group(1) + "Part" + match_part.group(2).upper()
            else:
                print("*** unexpected label pattern:", text, file=log)
    return year

def getRawYearList(soup, log):
    #print("in getRawYearList", file=log)
    yearList = []
    survey_year_container = soup.find("div", {"class": "custom-select select-hide"})
    if survey_year_container:
        li_elements = survey_year_container.find_all("li")
        for li in li_elements:
            a_tag = li.find("a")
            #print("found a tag", a_tag.text, file=log)
            if a_tag and "href" in a_tag.attrs:
                href = a_tag["href"]
                #print("href:", href, file=log)
                dropDownLabel = a_tag.text.strip()
                #print("dropDownLabel:", dropDownLabel, file=log)
                year = extractYear(dropDownLabel, log)
                if dropDownLabel is not None:
                    year_data = {
                        "dropDownLabel" : dropDownLabel,
                        "year": year,
                        "relative_href": href,
                        "folder": "year_"+year
                    }
                    yearList.append(year_data)
                    #print("year:", year, "original_href:", href, file=log)
    else:
        print("div container not found", file=log)
    #print(json.dumps(yearList, indent=2), file=log)
    return yearList

# The web page has the wrong urls for 1994PartA
# Using the links on the Part A page only gets you DS2 data not DS1 data
def fixup1994partAURL(relativeURL, log):
    newRelativeURL = re.sub(r'-DS0002-', '-DS0001-', relativeURL)
    print(f"relativeURL {relativeURL} adjusted to data set 1, is now {newRelativeURL}", file=log)
    return newRelativeURL


def getDownloadLinkLists(yearList, log):
    print("in getDownloadLinkLists", file=log)
    for entry in yearList:
        # intentionally to-console
        print("about to get download links for year:", entry["year"])
        pageUrl = "https://www.samhsa.gov" + entry["relative_href"]
        print("pageUrl:", pageUrl, file=log)
        folder = []
        downloadLinkList = []
        # get the collection page from its url
        pageSoup = None
        try:
            response = requests.get(pageUrl)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            response.encoding = 'utf-8'
            pageSoup = BeautifulSoup(response.content, "html.parser")
            #print("have collection page!", file=log)
        except FileNotFoundError:
            print(f"***Error: Page not found at {pageUrl}", file=log)
            continue
        except Exception as e:
            print(f"***An error occurred: {e}", file=log)
            continue

        if pageSoup is not None:
            # pull out the links to the docs
            documentation_heading = pageSoup.find("h3", class_="puf__label", string="Dataset Documentation")
            if documentation_heading:
                documentation_div = documentation_heading.find_next_sibling("div",
                                                                            class_="display-flex flex-column gap-105")
                if documentation_div:
                    for a_tag in documentation_div.find_all("a", href=True):
                        relativeURL = a_tag["href"]
                        if entry["year"] == "1994PartA":
                            relativeURL = fixup1994partAURL(relativeURL, log)
                        downLoadLink = "https://www.samhsa.gov"+ relativeURL
                        downloadLinkList.append(downLoadLink)
            else:
                print("no documentation heading", file=log)
            # pull out the links to the data files themselves
            downloads_heading  = pageSoup.find("h3", class_="puf__label", string="Dataset Downloads")
            if downloads_heading :
                downloads_div = downloads_heading.find_next_sibling("div",
                                                                            class_="grid-row")
                if downloads_div:
                    for a_tag in downloads_div.find_all("a", href=True):
                        relativeURL = a_tag["href"]
                        if entry["year"] == "1994PartA":
                            relativeURL = fixup1994partAURL(relativeURL, log)
                        downLoadLink = "https://www.samhsa.gov" + relativeURL
                        downloadLinkList.append(downLoadLink)
            else:
                print("no downloads heading", file=log)
            # if there are setup files, grab them also
            setups_heading = pageSoup.find("h3", class_="puf__label", string="ASCII Setup Files")
            if setups_heading:
                print("found setup files div", file=log)
                setups_div = setups_heading.find_next_sibling("div",
                                                                    class_="display-flex")
                if setups_div:
                    for a_tag in setups_div.find_all("a", href=True):
                        relativeURL = a_tag["href"]
                        if entry["year"] == "1994PartA":
                            relativeURL = fixup1994partAURL(relativeURL, log)
                        downLoadLink = "https://www.samhsa.gov" + relativeURL
                        downloadLinkList.append(downLoadLink)
                        #print("added setup file:", downLoadLink, file=log)
            else:
                print("no setups heading", file=log)

        folder.append(downloadLinkList)
        entry["downloadList"] = downloadLinkList
    return yearList



def countFiles(dict, log):
    totalDownloads = 0

    for year in dict["subfolderList"]:
        totalDownloads += len(year["downloadList"])

    print("have", totalDownloads, "URLs to download", file=log)
    return totalDownloads

def main():
    log = open("parse.log", "w", encoding="utf-8")
    print("in main, about to get page and instantiate soup", file=log)
    url = "https://www.samhsa.gov/data/data-we-collect/nsduh-national-survey-drug-use-and-health/datafiles"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.content, "html.parser")
        #use the code below to avoid beating the server during testing
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
    yearList = getRawYearList(soup, log)
    print("back from getRawYearList, num entries: ", len(yearList))
    #yearList = getCollectionPageLists(rawYearList, log)
    print("back from getCollectionPageLists")
    yearList = getDownloadLinkLists(yearList, log)
    print("back from getDownloadLinkLists")
    json.dump(yearList, log, indent=2, ensure_ascii=False)
    downloadDict = {
        "folder": "download",
        "subfolderList" : yearList,
        "downloadList": []  # don't expect any but let's be consistent
    }

    count = countFiles(downloadDict, log)
    print("total download links found: ", count)

    dictFilename = "downloadDict.json"
    try:
        with open(dictFilename, "w", encoding="utf-8") as jsonFile:
            json.dump(downloadDict, jsonFile, indent=2, ensure_ascii=False)
            print("dictionary dumped to:", dictFilename)
    except OSError as e:
        print(f"Error writing to file: {e}")

main()