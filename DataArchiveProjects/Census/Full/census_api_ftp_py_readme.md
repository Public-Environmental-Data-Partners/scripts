census_api_ftp_py_readme
The Census download scripts were used to download the whole Census catalog, with some files served by the API and some files served by the FTP site.  Before those scripts were run, there was a catalog created for each using census_api_catalog.py and census_ftp_catalog.py respectively.  

Everything run to the API uses an API key, and the API key has been stripped out of the scripts.  The user needs to insert their own API key, in the quotation marks, for the API scripts to run.

Both API and FTP download scripts reference the catalogs.  For the API, there are "flat" files and "grouped" files.  The original API download script included both, but then there were extensive problems with the flat files, so the two were separated and then the grouped files only were the ones obtained through API.  The flat files are all available on the FTP site (that's the microdata) and were thus captured through the FTP site instead.

FTP:  changed to allow concurrent downloads.  Here's what changed and why it should be significantly faster:
The progress line now shows download speed in MB/s and total GB so you can track throughput in real time. If you want to bump it up to 8 threads later, just change WORKERS = 8 at the top.