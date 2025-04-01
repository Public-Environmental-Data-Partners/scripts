The tools here were used to download pdf files from:
https://www.fisheries.noaa.gov/national/marine-mammal-protection/marine-mammal-stock-assessment-reports-species-stock

Those files were then archived at:
https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LBEFI8&version=DRAFT

To comply with stated Dataverse preferences on the number of files to be uploaded, the individual files
have been organized into folders that correspond to the headings/subheadings on the page
and then zipped. That should help anyone looking for a specific subset of the data.

Also added to this dataset is are three fairly rough-and-ready scripts that were used to generate it:
- parseMarineMammalypage.py -- parses the html and organizes the files to be downloaded
by folder/subfolder as written out to the downloadDict.json file.
- getMarineMammalsFile.py which takes the .json file as input and chunks through it
to create the folder structure and download the files. Note that it:
-- can be restarted as many times as needed; it skips any files that are already downloaded.
-- pauses 3 seconds between files to avoid overloading either the source website
or the local bandwidth -- controlled by a single variable you can find in the code
- zipForUploads.sh -- scripts the zipping of the top-level folders within the main
downloads folder created by getMarineMammalsFile.py
- downloadDictSample.json -- a renamed version of the working
file that is used to communicate between the two programs.