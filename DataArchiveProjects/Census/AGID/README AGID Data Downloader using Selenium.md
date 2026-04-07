AGID Data Downloader using Selenium

These scripts were written to have an automated assist in downloading AGID data from the Census, from the "Data Explorer".  There are three main categories (SPR Title III, PPR Title VI, and NORS Title VII).  



**V3** 

Allows user to choose category, geography and Data Elements.  That said, this was created for SPR Title III, which has 5 geographies, and the script prompts to cycle through each geography.  

Handles cycling through:

\- Multiple batches of 50 checkboxes per data element category

\- 5 geographies per batch

\- All data element categories from CSV

\- Renames downloaded files with structured naming convention

\- Creates lookup table CSV



**V4**

Changes from Version 3:  now only cycles between two geographies (States and All USA), since the other three geographies can be obtained through these two base geographic tabulations.  There is a check between geographies to prompt the user to either cycle to the next geography, or select next 50 checkboxes, or choose different data elements, or quit.  In order to change categories, the user needs to choose "quit" and start over again and choose the new category.  



Handles cycling through:

\- Multiple batches of 50 checkboxes per data element category

-2 geographies per batch

\- All data element categories from CSV

\- Renames downloaded files with structured naming convention

\- Creates lookup table CSV

