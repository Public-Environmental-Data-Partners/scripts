The data in the files archived here are believed to be a superset of the data
needed to power FLIGHT (the Facility Level Information on GreenHouse gases Tool)
https://ghgdata.epa.gov/ghgp/main.do
and at least some of the other, multiple web and query interfaces to greenhouse 
gas reporting data. See https://www.epa.gov/enviro/greenhouse-gas-overview 
for links to those tools.
But that belief has not yet been rigorously validated.
Please approach this dataset with trust-but-verify attitude.

The CSV data files contained in the zips were downloaded using the DMAP GraphQL 
API available at: https://www.epa.gov/enviro/dmap-graphql-api

See the word doc EPAGreenhouseGasSearchAPIForAutomation.docx in this folder 
for a specification for the code that was written to use the API. 

Step 2 in that document was used to generate a CSV file of the tables 
available for download and the number of records in each. That file is 
saved here as:
ghgTableCountSorted.csv

Note that the file above lists 370 tables. However, you will find 371 files
among the zip files here.
- 356 files were successfully downloaded using the dmap-graphql-api approach
documented in the docx file
- 14 files, which all list 0 record counts in the .csv file above, did not
download successfully via the api and, for at least some of them, their 
Model web pages say the api cannot be used with them. Whether they are 
truly empty or considered too big to download is not known. 
However, a csv file was created for each of those tables containing simply
a row of column headers from the field names documented on their Model pages.
See the list of these files below.
- 1 file, pub_dim_facility.csv, which has its own zip file, was downloaded 
using the api call: https://data.epa.gov/dmapservice/ghg.pub_dim_facility/csv
This file is apparently common to all the Envirofacts subject areas.


"Empty" csv files created from documented field names:
aa_tier1equationinput
ee_cems_details
ee_cems_info
ee_tier4cems_qtrdtls
mv_ef_i_fab_stack_sys_details
mv_ef_i_fab_stack_test_cert
k_cems_details
k_cems_info
s_no_cems_annual_avg
tt_waste_depth_details
ef_w_comp_workovers_no_frc_del
ef_w_eor_injection_missing
z_cems_details
z_tier4cems_qtrdtls
