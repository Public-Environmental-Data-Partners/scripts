 #!/bin/bash
 # dumb scripts for just downloading as much static data as I can
  wget -e robots=off -r -np -k "https://historicalcharts.noaa.gov/includes/imageDBDT.php?title=&chart=&yearMin=&yearMax=&singleYear=&type=Any%20Type&state=Any&scale=All%20Scales&latitude=&longitude=&js=yes"
 # For pdfs
 wget -e robots=off --recursive  "https://historicalcharts.noaa.gov/includes/pubDBDT.php?title=&yearMin=&yearMax=&singleYear=&type=Any%20Type&keyword=&docnum="
 # wget -e robots=off -r -np -k "https://historicalcharts.noaa.gov/includes/pubDBDT.php?title=&yearMin=&yearMax=&singleYear=&type=Any%20Type&keyword=&docnum="
# wget -e robots=off -r -np -k https://historicalcharts.noaa.gov/search.php?search=*
#wget -e robots=off -r -np -k https://historicalcharts.noaa.gov
