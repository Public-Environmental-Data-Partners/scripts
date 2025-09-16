# Code to parse a series of web pages starting from:
# https://www.samhsa.gov/data/data-we-collect/nsduh-national-survey-drug-use-and-health/methodology
# To be compatible with the getFiles.py code, the code needs to output a json structure
# to be used by getFiles.py.
# The dictionary must have a series of nested folder dictionaries where, for each folder, there are the following
# elements:
#  folder -- a valid file folder name
#  subfolderList -- a list of dictionaries like this one for each subfolder, may be empty
#  downloadList -- a list of absolute URLs to files to be downloaded, may be empty
# In addition, the dictionary entries can have any working data values of use to the coder. All will be ignored.
# See sampleJsonDict.json as an example.
from pydoc import pager

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
    if text[0] == '/':
        text = text[1:]
    text = re.sub(r'\s*\(.*?\)', '', text)  # Remove space and parentheses
    text = re.sub(r'[^\x00-\x7F]+', '', text) # Remove non-ASCII characters
    text = re.sub(r'[/]', '_', text)  # Replace folder separators with underscores
    text = re.sub(r'[.,;:"\']', '', text)  # Remove unwanted chars
    text = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', text)
    text = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', text)
    text = text.replace(" ", "_").replace("-", "_") # replace spaces with underscores
    text = text.replace("-", "_")  # replace hyphens with underscores
    text = re.sub(r'_+', '_', text)  # Remove multiple underscores
    return text.lower()

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
                        "folder": "year_"+year+"_methodology"
                    }
                    yearList.append(year_data)
                    print("year:", year, "original_href:", href, file=log)
    else:
        print("div container not found", file=log)
    print(json.dumps(yearList, indent=2), file=log)
    return yearList

def getDownloadLinkListsFromModernPages(yearList, log):
    print("in getDownloadLinkLists", file=log)
    for entry in yearList:
        # this logging is intentionally to-console
        print("about process entry year:", entry["year"])
        pageUrl = getPageUrl(entry["relative_href"], log)
        folder = []
        downloadLinkList = []
        pageSoup = getWebPage(pageUrl, log)

        if pageSoup is not None:
            pageToSave = getPageToSave(entry, pageUrl)
            entry["pageToSave"] = pageToSave

            # This is considerably simpler than the logic I've been using.
            # But for all of the data before the "older" 2010-2014 page, this certainly seems to be working.
            download_links = pageSoup.find_all('a', class_='file-icon')
            for link in download_links:
                relativeURL = link.get('href')
                if ".pdf" in relativeURL or ".htm" in relativeURL:
                    #print("found downloadable file: ", relativeURL, file=log)
                    downloadLinkList.append("https://www.samhsa.gov" + relativeURL)
                else:
                    print("found non-download link:", relativeURL, file=log)

            folder.append(downloadLinkList)
            entry["downloadList"] = downloadLinkList
    print("all normal year entries have been processed")
    return yearList


def getPageToSave(entry, pageUrl):
    # we will want to save this web page for provenance
    webPageFileName = "./toUpload/" + text2validFileFolderName(entry["relative_href"]) + ".html"
    pageToSave = {
        "url": pageUrl,
        "filename": webPageFileName
    }
    return pageToSave


def getWebPage(pageUrl, log):
    # get the web page from its url
    pageSoup = None
    try:
        response = requests.get(pageUrl)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = 'utf-8'
        pageSoup = BeautifulSoup(response.content, "html.parser")
        print("have page:", pageUrl, file=log)
    except FileNotFoundError:
        print(f"***Error: Page not found at {pageUrl}", file=log)
    except Exception as e:
        print(f"***An error occurred: {e}", file=log)
    return pageSoup


def getPageUrl(relative_href, log):
    pageUrl = "https://www.samhsa.gov" + relative_href
    # print("pageUrl:", pageUrl, file=log)
    return pageUrl


def getDownloadLinksFromOlderPages(yearList, log):
    print("in getDownloadLinksFromOlderPages", file=log)
    # I hate to hardwire these. But I've spent waaaaay too much time trying to
    # parse them from https://www.samhsa.gov/data/data-we-collect/nsduh-national-survey-drug-use-and-health/methodology/older
    olderYearList = [
        {
            "year": "2014",
            "relative_href": "/data/report/nsduh-2014-methodological-resource-book-mrb",
            "folder": "year_2014_methodology"
        },
        {
            "year": "2013",
            "relative_href": "/data/report/nsduh-2013-methodological-resource-book-mrb",
            "folder": "year_2013_methodology"
        },
        {
            "year": "2012",
            "relative_href": "/data/report/nsduh-2012-methodological-resource-book",
            "folder": "year_2012_methodology"
        },
        {
            "year": "2011",
            "relative_href": "/data/report/nsduh-2011-methodological-resource-book",
            "folder": "year_2011_methodology"
        },
        {
            "year": "2010",
            "relative_href": "/data/report/nsduh-2010-methodological-resource-book-mrb",
            "folder": "year_2010_methodology"
        }
    ]
    for entry in olderYearList:
        # this logging is intentionally to-console
        print("about process older year:", entry["year"])
        pageUrl = getPageUrl(entry["relative_href"], log)
        downloadLinkList = []
        # the links we need are paginated by default and I can't find a way
        # to get them to all display on one page, so
        pageList = ["0", "1"]
        for num in pageList:
            paginatedUrl = pageUrl + "?page=" + num
            print("paginatedUrl:", paginatedUrl, file=log)
            pageSoup = getWebPage(paginatedUrl, log)
            if pageSoup is not None:
                if num == "0":
                    pageToSave = getPageToSave(entry, pageUrl)
                    entry["pageToSave"] = pageToSave

                linksToFilePages = [a['href'] for a in pageSoup.select('.view-content .views-row .views-field-title a')]
                for link in linksToFilePages:
                    #print("link to page with download:", link, file=log)
                    downloadPageUrl = getPageUrl(link, log)
                    downloadSoup = getWebPage(downloadPageUrl, log)
                    if downloadSoup is not None:
                        downloadAnchor = downloadSoup.find('div', class_='download-file-link').find('a')
                        if downloadAnchor is not None:
                            downloadHref = downloadAnchor.get('href')
                            downloadLink = getPageUrl(downloadHref, log)
                            downloadLinkList.append(downloadLink)
                    else:
                        print(f"***Error: Page not found at {downloadPageUrl}", file=log)
            else:
                print(f"***Error: Page not found at {paginatedUrl}", file=log)
        entry["downloadList"] = downloadLinkList
        yearList.append(entry)
    return yearList


def countFiles(list, log):
    totalEntries = 0
    totalDownloads = 0

    for entry in list:
        totalEntries += 1
        totalDownloads += len(entry["downloadList"])

    print("\nhave", totalDownloads, "URLs to download from", totalEntries, "total entries", file=log)
    return totalDownloads

def main():
    log = open("parse.log", "w", encoding="utf-8")
    print("in main, about to get page and instantiate soup", file=log)
    url = "https://www.samhsa.gov/data/data-we-collect/nsduh-national-survey-drug-use-and-health/datafiles"
    try:
        # response = requests.get(url)
        # response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        # response.encoding = 'utf-8'
        # soup = BeautifulSoup(response.content, "html.parser")
        #use the code below to avoid beating the server during testing
        sampleFile = open("firstMethodologyPage.html", "r", encoding="utf-8")
        soup = BeautifulSoup(sampleFile, "html.parser")
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
    print("back from getRawYearList, num entries: ", len(yearList), file=log)
    # raw year list is expected to include one entry we want to ignore/
    # process a different way
    if yearList[-1]["relative_href"].endswith("older"):
        del yearList[-1]
        print("older entry removed from yearList", file=log)
    yearList = getDownloadLinkListsFromModernPages(yearList, log)
    print("back from getDownloadLinkLists")
    yearList = getDownloadLinksFromOlderPages(yearList, log)
    print("back from getDownloadLinksFromOlderPages")
    json.dump(yearList, log, indent=2, ensure_ascii=False)
    downloadDict = {
        "folder": "download",
        "subfolderList" : yearList,
        "downloadList": []  # don't expect any but let's be consistent
    }

    count = countFiles(downloadDict["subfolderList"], log)
    print("\ntotal download links found: ", count)

    dictFilename = "methodologyDownloadDict.json"
    try:
        with open(dictFilename, "w", encoding="utf-8") as jsonFile:
            json.dump(downloadDict, jsonFile, indent=2, ensure_ascii=False)
            print("dictionary dumped to:", dictFilename)
    except OSError as e:
        print(f"Error writing to file: {e}")

main()