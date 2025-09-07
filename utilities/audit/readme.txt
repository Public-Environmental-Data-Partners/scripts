# Audit utilities
This folder is intended to hold any utility scripts we write for auditing/managing
Dataverse entries.

- generateAuditList.py
This script uses two dataverse APIs, Search (for locating datasets) and
Native (for extracting details about each).
It currently finds all *published* datasets and then generates a .csv 
file with a minimal set of information about each including Title,
Publication Date, Depositor, Deposit Date, and CAFE URL (doi)