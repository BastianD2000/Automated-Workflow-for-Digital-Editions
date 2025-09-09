# Automated Workflow for Digital Editions

This repository contains the code developed in the context of my Bachelor's thesis. 
The project automates repetitive steps in the creation and publication of digital editions.

The workflow automates processing steps by using the Transkribus Legacy API.
Digitized images can be uploaded to Transkribus, layout analysis and OCR automatically start and the final result gets exported and downloaded.
eXist servers are integrated for further processing and the final publication.
GitLab issues can be used for automatic logging of workflow runs.

'transkribus_tasks.py' contains the functions for interacting with the Transkribus API.
'transkribus_main.py' orchestrates the functions in a prefect workflow.
'git_tasks.py' provides the functions for logging by GitLab-Issue and for pushing data to repositories.
'exist_tasks.py' contains functions for interacting with eXist servers.
'helper_tasks.py' contains a function for error handling and one for XML validation.
