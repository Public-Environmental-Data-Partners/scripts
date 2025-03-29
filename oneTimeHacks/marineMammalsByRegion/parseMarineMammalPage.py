# Code to get data from:
# https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-region
# This is closely related to but distinct from the data at:
#    https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-species-stock

from bs4 import BeautifulSoup, Tag
import json
import os
from pathlib import Path
import re
import requests
import time
import urllib.parse


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

def getPdfFromIndirectLink(href, log):
    pdfLink = None
    # the href URL may be a partial one -- if so, see if we can complete it
    if href.startswith("/resource"):
        href = "https://www.fisheries.noaa.gov" + href
        print("got partial url in href, will try get with:", href, file=log)
    # get the new page we are directed to
    print("in getPdfFromIndirectLink, about to get:", href, file=log)
    try:
        response = requests.get(href)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        indirectSoup = BeautifulSoup(response.content, "html.parser")
    except FileNotFoundError:
        print(f"Error: Page not found at {href}", file=log)
        return pdfLink
    except Exception as e:
        print(f"An error occurred: {e}", file=log)
        return pdfLink
    print("survived get and instantiation", file=log)
    actionUrl = getActionUrlFromForm(href, indirectSoup, log)
    if actionUrl is not None:
        pdfLink = "https://repository.library.noaa.gov" + actionUrl
        print("got pdfLink from action url", pdfLink, file=log)
    else:
        print("will try to get url from large button", file=log)
        # If there's no form on the page, then this may be one of those
        # two-step indirections with a button here to the page with the
        # form. But we should be able to short circuit the two-page traverse
        # and compose the pdf name from here.
        button = indirectSoup.find("a", class_="button button--primary button--large-action")
        if button and button.has_attr("href"):
            href = button["href"]
            #print("href from non-action form button:", href, file=log)
            # We're depending on NOAA to be consistent in their file naming convention.
            # But as far as I can see by inspection, they have been consistent
            pdfLink = f"{href}/noaa_{href.split('/')[-1]}_DS1.pdf"  # chatGpt code, a bit terse for me
            print("generated pdfLink from non-action form button", pdfLink, file=log)
        else:
            print("did not find large primary button", file=log)
    if pdfLink is not None:
        print("found indirect PDF link:", pdfLink, file=log)
    return pdfLink


def getActionUrlFromForm(href, indirectSoup, log):
    # parse the indirect page to find a form with a Download button
    # and pull the pdf link out of the form action attribute
    actionUrl = None
    form = indirectSoup.find("form", id="download-document")
    if form and form.has_attr("action"):
        actionUrl = form["action"]
        print("found action URL:", actionUrl, file=log)
    else:
        print("***no action URL found on:", href, file=log)
    return actionUrl


def getReportsByYear(h2, log):
    yearName = h2.text.strip()
    print("in getReportsByYear for year:", yearName, file=log)
    yearDict = {
        "year" : yearName,
        "folder" : text2validFileFolderName(yearName),
        "folderLevel": 1,
        "subfolderList" : []
    }
    next_sibling = h2.find_next_sibling()
    print("starting region:", next_sibling, file=log)
    while next_sibling:
        if isinstance(next_sibling, Tag):
            if next_sibling.name == "h2":
                #print("found next h2", file=log)
                break
            if next_sibling.name == "h3":
                #print(f"H3: {next_sibling.text}", file=log)
                #print("found region:", next_sibling, file=log)
                regionName = next_sibling.text.strip()
                #print("year name", yearDict["year"], " -- region name:", regionName, file=log)
                regionDict = {
                    "region": regionName,
                    "folder": text2validFileFolderName(regionName),
                    "folderLevel": 2,
                    "subfolderList" : [], # don't expect any but let's be consistent
                    "downloadList": [],
                    "getByHandList": []
                }
                ul_tag = next_sibling.find_next_sibling("ul")
                if ul_tag:
                    #print("found ul_tag:", ul_tag, file=log)
                    li_tags = ul_tag.find_all("li")
                    #print("found", len(li_tags), "li_tags", file=log)
                    for li in li_tags:
                        #print("processing li_tag:", li, file=log)
                        a_tag = li.find("a")
                        if a_tag is not None:
                            href = a_tag.get("href")
                            #print(f"    Href: {href}", file=log)
                            # some pdf links have weird null query parameters at the end, such as
                            # https://media.fisheries.noaa.gov/2021-07/Pacific%202020%20Summarytable.pdf?null%09
                            # Let's strip those off before we test for a direct pdf link
                            href = href.split("?")[0]
                            if href.endswith(".pdf"):
                                regionDict["downloadList"].append(href)
                            else:
                                pdfLink = getPdfFromIndirectLink(href, log)
                                if pdfLink is not None:
                                    regionDict["downloadList"].append(pdfLink)
                                else:
                                    regionDict["getByHandList"].append(href)
                yearDict["subfolderList"].append(regionDict)
        next_sibling = next_sibling.find_next_sibling()
    return yearDict

def countFiles(dict, log):
    totalDownloads = 0
    totalGetByHand = 0

    for year in dict["subfolderList"]:
        for region in year["subfolderList"]:
            totalDownloads += len(region["downloadList"])
            totalGetByHand += len(region["getByHandList"])
            if (len(region["getByHandList"]) > 0):
                print(year["year"], region["region"], "files to get by hand:", file=log)
                for name in region["getByHandList"]:
                    print(name, file=log)

    return totalDownloads, totalGetByHand

def main():
    log = open("parse.log", "w", encoding="utf-8")
    print("in main, about to get page and instantiate soup", file=log)
    url = "https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-region"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.content, "html.parser")
        # use the code below to avoid beating the server during testing
        #with open("Marine Mammal Stock Assessment Reports by Region _ NOAA Fisheries.html", "r", encoding="utf-8") as input:
        #    soup = BeautifulSoup(input, "html.parser")
    except FileNotFoundError:
        print(f"Error: Page not found at {url}")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(2)
    print("back from file open and soup instantiation", file=log)
    print("page title is: ", soup.title, file=log)
    h2List = soup.find_all("h2")
    yearList = []
    for h2 in h2List:     # slice here for shorter tests
        h2Title = h2.text.strip()
        print(f"h2Title: {h2Title}", file=log)
        if (h2Title != "On This Page") and (h2Title != "More Information"):
            yearDict = getReportsByYear(h2, log)
            yearList.append(yearDict)
            print("back from getReportsByYear, yearList count:", len(yearList), file=log)
        else:
            print(f"not processing: |{h2Title}|", file=log)

    #print(yearList, file=log)
    downloadDict = {
        "folder": "download",
        "folderLevel": 0,
        "subfolderList" : yearList,
        "downloadList": [],  # don't expect any but let's be consistent
        "getByHandList": []

    }

    totalDownloads, totalGetByHand = countFiles(downloadDict, log)
    print("Total files in downloadLists:", totalDownloads, file=log)
    print("Total entries in getByHandLists:", totalGetByHand, file=log)

    dictFilename = "downloadDict.json"
    try:
        with open(dictFilename, "w", encoding="utf-8") as jsonFile:
            json.dump(downloadDict, jsonFile, indent=2, ensure_ascii=False)
            print("dictionary dumped to:", dictFilename)
    except OSError as e:
        print(f"Error writing to file: {e}")

main()