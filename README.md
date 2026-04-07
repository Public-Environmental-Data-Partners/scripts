# PEDP scripts

*This is not your normal GitHub repository.* 

Instead of containing a single project with a coherent structure, 
it is intended to allow any coders who are:
- archiving data sets for the Public Environmental Data Project 
(https://www.openenvironmentaldata.org/pilot-type/public-environmental-data-project) OR
- building utilities for audit/maintenance of the Dataverse
to upload their working scripts both for safe-keeping and for
others to reference and possibly build upon. 

Eventually, the code in this repo may evolve into more coherent structure
of tools. For now, however, 
- small scraping/archiving code should simply be added in 
folders under _/DataArchiveProjects_ and
- auditing and other scripts should be added under _/utilities_

## Adding code
If you have built code in the course of your work on a PEDP project, 
we strongly encourage you to share it here. It does not need to be
perfect or even polished (read some of the existing code if you need
to be convinced of that ;-). If code was useful to you, it may save
another contributor or your future self some time and trouble to be
able to find it here as a starting point for another project.

In general, follow the steps in the [Contributing guide](CONTRIBUTING.md), 
specifically for making [code changes](https://github.com/Public-Environmental-Data-Partners/overview/blob/main/CONTRIBUTING.md#code-and-documentation-changes) 
using Pull Requests. In addition, here are some notes specific to the folders in this repo.

### DataArchiveProjects
If you add code here, please:
- create a folder for your code, use your own best judgement for a folder name
- along with any working code, add a _readme.txt_ file that documents:
    - the URL of the primary web page you were archiving
    - the URL of the Dataverse (or other online repository) where your archived data was stored
    - any other notes you want to make about the code you are archiving

(Note also that, while it is not currently required, it has become a common and recommended practice
to bundle the code you used for an archving project into a zip file that is archived
with the data.)

### utilities
For code that is not associated with a specific archiving project, please
add it here. You're welcome to add code to any existing folder or,
if none apply, to create a folder of your own. If you add code to an existing
folder, please update the _readme.txt_ there. If you create a new folder, 
please include a _readme.txt_ file

