EPA Science Inventory Downloads Readme

The downloads from the Science Inventory (SI) occurred using a set of scripts that scraped the website, then downloaded, and then catalogued. The website is separated into three main categories:

\- Reports

\- Presentations

\- All others

All of the types were downloaded together. The contents of each category are also presented in duplicate using their category, for findability. However, the exception to this are the NEPIS files (National Service Center for Environmental Publications, also known as NSCEP). These files have a special download mechanism, where downloads are restricted by the hour and frequency of download request. Thus, it is not clear that all NEPIS files are captured.

The files are presented in archives by category (report, presentation, NEPIS), and then by page count, and then by all together. For the latter, the indexes are the combined 7 indexes as listed below.

For files with duplicate filenames, the title of the item was used (from the scraped records), with “filler” words stripped out.

The downloads have been broken into chunks by “pages” of returns from the SI site. The scraper catalogued into pages of 25 records each, and thus the download process was handled by page count:

1 – 100

101 – 500

501 – 1000

1001 – 1250

1251 – 2000

2001 – 3000

3001 – 4000

Scripts:

**epa_SI_uncat_scraper1.py** (Oct 29, 16KB)

Purpose: Scrapes EPA Science Inventory for all document types EXCEPT journals, creates a CSV index

Scope: Configurable page ranges (default 1-10)

Features: Extracts metadata and download URLs from multiple document types (reports, books, data/software, etc.)

Main Features:

1\. \*\*Multiple URL Columns\*\*: Auto-detects columns starting with 'url' (or you can specify them manually). Handles up to 31 URL columns.

2\. \*\*Intelligent Filename Logic\*\*:

\- \*\*Single download per row\*\*: Uses native filename

\- \*\*Multiple downloads per row\*\*: Adds title prefix (first 4 non-filler words)

\- \*\*Duplicate filenames across records\*\*: Adds letter suffix (a, b, c, etc.)

3\. \*\*Filler Word Filtering\*\*: Removes common filler words (to, the, of, and, at, in, for, a, an) when creating title prefixes.

4\. \*\*Two-Pass Processing\*\*:

\- Pass 1: Collects all download information

\- Pass 2: Downloads files and applies appropriate naming

\## Key Changes from Original:

\- \`download()\` now returns \`(success, native_filename)\` tuple

\- New \`extract_title_prefix()\` function for creating prefixes

\- Tracks filename usage with \`filename_counter\` and \`filename_usage\` dictionaries

\- Automatically renames files after download based on conflict detection

\- Provides a conflict report at the end showing which filenames were duplicated

\## Usage:

\`\`\`python

process_csv(

csv_file="your_file.csv",

url_columns=None, # Auto-detect or specify \['url', 'url1', 'url2', ...\]

title_column='title', # Column with record titles

output_dir='downloads',

max_rows=None # For testing, set to a number

)

**epa_multi_url_and_nepis_download3.py** (Nov 5, 28KB)

Purpose: Purpose: Downloads files from CSV created by scrapers + handles NEPIS URLs. Enhanced version of the downloader with better NEPIS handling

Key feature: Has get_nepis_download_url() function that parses NEPIS popup pages to find actual PDF download links

NEPIS handling: Constructs popup URL (Display=p%7Cf), parses HTML for PDF link, handles JavaScript patterns using BeautifulSoup/requests

Key additions: Handles multiple URL formats per record; Better filename sanitization; Title-based prefixes for files; Duplicate tracking; Statistics by domain

**epa_nepis_parse_manual1.py** (Nov 6, 17KB)

Purpose: Separates NEPIS URLs from regular downloads for manual processing

Key feature: Creates separate CSV files for NEPIS vs non-NEPIS URLs

Use case: When automated NEPIS parsing fails, this prepares files for manual download

The workflow was:

epa_SI_uncat_scraper1.py - Generate index of all documents

epa_multi_url_and_nepis_download3.py - Download most files (with best NEPIS handling)

epa_nepis_parse_manual1.py - For stubborn NEPIS files that fail automation, separate them for manual download

Scraper scripts are available in the archive, and in GitHub.

As of 11/6/2025

\- Scraped 94,031 entries

\- Of those, 1,718 are have NEPIS links associated with them

\- Of the non-NEPIS entries (92,313), there are 12,002 files from entries.

\- Of the NEPIS associated entries (that is, NEPIS links in any download column), 225 were download urls in columns that were not NEPIS, and of those, only 70 were actual downloads. The remainder of the 225 did not contain valuable content (that is, they either have “NTIS contact” listed, or a “follow the URL” sentence, or instruction to “contact the Program Officer” as the only page in a pdf.

Using an Advanced Search for a space (“ “) returned 94,428 records. (Same results when searching for no space, or an underscore “\_”).

Advanced Search for each file type, with wildcard “space” ( )

232 ASSESSMENT DOCUMENTs

1380 BOOKs

1340 BOOK CHAPTERs

276 COMMUNICATION PRODUCTs

25 CRITERIA DOCUMENTs

0 DATA/SOFTWARE

177 EPA PUBLISHED PROCEEDINGS

233 ETV DOCUMENTs

147 EXTRAMURAL DOCUMENTs

232 IRIS ASSESSMENTs

23395 JOURNALs

289 NEWSLETTERs

191 NEWSLETTER ARTICLEs

0 NON-EPA PUBLISHED PROCEEDINGS

15 PAPER IN EPA PROCEEDINGS

0 PAPER IN NON-EPA PROCEEDINGS

34077 PRESENTATIONs

4029 PUBLISHED REPORTs

5 RISK ASSESSMENT GUIDELINES

2516 SCIENCE ACTIVITies

524 SITE DOCUMENTs

407 SUMMARies

(subtotal: 69,490)

24,938 = uncategorized records