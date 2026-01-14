Dataverse Uploader README
Automates the uploading of files to Dataverse.  Especially useful for automating large numbers of files; the limits of file size are still in play.

Two scripts:  
dataverse_upload.py
dataverse_upload_with_zip.py

You need a Dataverse API token, plus a record page with a DOI to upload to.  Then there is a flag in the dataverse_upload.py script for ignoring zip files (all formats) = change this to "FALSE" if you'd like to include zip files.  Or use the dataverse_upload_with_zip.py to upload with zip files included.  

There's no log for failures, so if you need that you'll have to add that in.  

Two approaches (same for both scripts) for usage:

Option 1: Edit the config in the file (lines 27-34)
python dataverse_upload.py

Option 2: Specify everything via command line
python dataverse_upload.py --url https://dataverse.harvard.edu --token YOUR_TOKEN --pid doi:10.7910/DVN/12345 --dir C:/path/to/files

Short flags also work:

python dataverse_upload.py -u https://dataverse.harvard.edu -t YOUR_TOKEN -p doi:10.7910/DVN/12345 -d C:/path/to/files
Mix and match (edit some in file, override others on command line)

# If you set URL and token in the file, just override dir and pid
python dataverse_upload.py --pid doi:10.7910/DVN/67890 --dir C:/new_folder

Get help:

python dataverse_upload.py --help
The command line arguments will override whatever is in the config section of the file