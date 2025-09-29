The scripts here are organized into two folders:

- ghgDownloads -- these are the scripts that were used to directly find, download/generate,
and prep the ghg files that were archived in Dataverse entry
https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/6V0F7S

Listed alphabetically:

  -- downloadColumnNamesForTablesWithNoRecords.py -- script to	   generate place-holder csv files
       for the 14 empty tables that would not download via the dmap-graphql-api
  -- downloadGhgDbFilesViaJson.py -- script to download the actual
     db tables as csv files (api returns json, code reformats to csv)
  -- EPAGreenhouseGasSearchAPIForAutomation.docx -- Michael Cohen's summarization of the
     various API calls available for harvesting GHG data and how to use them. This was the
	 primary specification for ghgModelFromDMAPapi.py
  -- ghgModelFromDMAPapi.py -- This code finds all the ghg tables and generates an output csv
     with the table names and counts to be used by the two download scripts
  -- readme.txt -- readme file that was uploaded to the Dataverse with the ghg files and that
     explains the different subsets within them
  -- zipForUpload.sh -- zips the downloaded data files into subpart archives
  
- ghgDeadEnds
  There are four scripts here that represent various early attempts
  to get a complete picture of the GHG db model (parseModelPagesViaSelenium.py), download the files documented via
  the model (downloadGhgDbFilesViaCsv.py), and get empty tables to download headers (downloadGhgDbFilesViaJsonWLimit0Fallback.py and
  getEmptyTableColumns.py). 
  
  I'm reluctant to simply abandon these on my local drive since 
  history tells us that we may yet have much to learn about this 
  database and how to get a truly comprehensive file set.

