# ImportSO

This script imports questions and answers from Stack Overflow into a SQLite database

## Prerequisites
* SQLite 3
* Python 2 (tested in 2.6)

## Setup
1. Create a database in SQLite 3 using `create_schema.sql`
2. Change DB_PATH in importso.py accordingly

## Usage
`python importso.py start_date end_date` (inclusive) where the date format is YYYY/MM/DD

* The database schema is as described in `create_schema.sql`
* Tags are stored as a comma and space delimited string, e.g. `java, android, sqlite`

## Limitations
* The script hasn't been extensively tested and doesn't proactively handle potential failures. In particular, it will probably fail if it the data received from the API isn't as expected.
* For performance reasons, it is assumed that questions / answers with higher IDs come later. That is, only items with IDs greater than the largest ID in the database at the beginning of script execution would be added.
* Since questions and answers are obtained separately, it is possible that the question corresponding to an answer would not be in the database
