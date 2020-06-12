<h1>Project 1 Data Engineer Nanodegree: Data Modeling with Postgres</h1>

<h3>Introduction</h3>
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

<h3>Project Description</h3>
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

<h3>Schema of PostgreSQL sparkify table</h3>

<b>Fact Table</b>
* <b>songplays</b> - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>
* <b>users</b> - users in the app
user_id, first_name, last_name, gender, level

* <b>songs</b> - songs in music database
song_id, title, artist_id, year, duration

* <b>artists</b> - artists in music database
artist_id, name, location, latitude, longitude

* <b>time</b> - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

![Alt text](resources/Star_schema.png?raw=true "Star Schema of DB")

<h3>Purpose of Schema</h3>
The purpose of previously depicted schema is to have each entry as normalized as possible (3 normal form)
in order to make writting as easy as possible (reduce redundancy and increase integrity) and
to make analyzing tasks possible with introducing a fact table to easy execute queries without JOINS being needed.

<h3>How to</h3>
* Execute <b>create_tables.py</b> with "python create_tables.py" to create and populate DB table
* Execute etl.ipynb Jupyter Notebook to run etl pipeline with direct notifications of results e.g.
populate DB with content of song and log JSONs inside /data
* Execute test.ipynb Jupyter Notebook to validate proper creation of PostgreSQL Table Sparkify
* sql_queries.py holds all the insert, delete and the specific song select query asked by the company