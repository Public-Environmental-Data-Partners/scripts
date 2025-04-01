The tools here were used to download 1961 .zip files on the Pre-generated Data files 
web page (https://aqs.epa.gov/aqsweb/airdata/download_files.html)

Those files were then archived at:
https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FD2W39C&version=DRAFT

To comply with stated Dataverse preferences on the number of files to be uploaded, the individual files
have been organized into folders 
and then zipped. 

There were 1961 .zip files on the Pre-generated Data files 
web page (https://aqs.epa.gov/aqsweb/airdata/download_files.html)
These were listed in the file files.csv I downloaded from the same page


I chose to group all the readings for a single statistic (particulate?)
into one archive. So, for example, the 25 files named
8hour_42101_1980.zip to 8hour_42101_2024.zip are archived in one
file named 8hour_42101_all_years.zip.

I used a bash script to generate my grouped zips, so I'm reasonably
certain I got all of the original files.

In addition to the normal pdf and html versions of the web page, the
following files were also archived:
files.csv -- see above
aqs_monitors.zip -- monitor listing
aqs_sites.zip -- site listing