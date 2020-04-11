# Intro

Startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Description

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

## Files in the repository


* **data**: Raw data (JSON)
* **create_tables.py**: (re-)Create database and tables
* **etl.py**: Fill the database
* **sql_queries.py**: Query patterns

## Run scripts

```bash
python3 create_tables.py
python3 etl.py
```
