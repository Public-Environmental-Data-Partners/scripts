# Code to get data from:
# https://aqs.epa.gov/aqsweb/airdata/download_files.html

import csv
from datetime import datetime
from pathlib import Path
import requests

logFile = open("download2.log", "w")
startTime = datetime.now()
print(f"Start time: {startTime}")
print(f"Start time: {startTime}", file=logFile)

# Our input data lives one folder up from where we are executing
# This was downloaded from the website first.
# Enhance?: get the file directly from the website rather
# that require a manual first step
root_folder = Path(__file__).parent.parent
inputFileList = root_folder.joinpath("file_list.csv")

fileList = []

# Read the file and parse the data
with open(inputFileList, newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header line

    for row in reader:
        if row:  # Ensure the row isn't empty
            fileList.append(row[0])  # First field

print("found", len(fileList), "file names", file=logFile)
print("first name:", fileList[0], file=logFile)
print("last name:", fileList[-1], file=logFile)

baseURL = "https://aqs.epa.gov/aqsweb/airdata/"
downloadCount = 0
errorCount = 0
skipCount = 0
for entry in fileList:
    localPath = root_folder.joinpath(entry)
    if not localPath.exists():
        downloadURL = baseURL + entry
        print("about to try: ", downloadURL, file=logFile)
        response = requests.get(downloadURL)
        if response.status_code == 200:  # 200 means the file exists
            with open(localPath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("File written successfully to: ", localPath, file=logFile)
            downloadCount += 1
            print("proof of life, download count is: ", downloadCount)
        elif response.status_code == 404:
            print("File not found.", file=logFile)
            errorCount += 1
        else:
            print(f"Request failed with status code:", response.status_code, file=logFile)
            errorCount += 1
    else:
        print("Already exists:", localPath, "skipping download this time.", file=logFile)
        skipCount += 1

print(downloadCount, "files downloaded.", file=logFile)
print(errorCount, "errors encountered.", file=logFile)
print(skipCount, "files skipped.", file=logFile)

endTime = datetime.now()
elapsedTime = endTime - startTime
print(f"End time: {endTime}")
print(f"Elapsed time: {elapsedTime}")
print(f"End time: {endTime}", file=logFile)
print(f"Elapsed time: {elapsedTime}", file=logFile)


logFile.close()