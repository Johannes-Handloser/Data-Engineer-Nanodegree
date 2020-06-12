<h1>Project 3 Data Engineer Nanodegree: Data Warehouse</h1>

<h3>Introduction</h3>
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.
 Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift,
and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify 
and compare your results with their expected results.
<h3>Project Description</h3>
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift.
To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

<h3>Datasets</h3>
* Song Data Path: s3://udacity-dend/song_data 
* Log Data Path: s3://udacity-dend/log_data 
* Log Data JSON Path: s3://udacity-dend/log_json_path.json

See also http://millionsongdataset.com/ for song data source

<h3>Schema of Redshift Data Warehouse</h3>

<b>Fact Table</b>
* <b>songplays</b> - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
<b>Dimension Tables</b>
* <b>users</b> - users in the app user_id, first_name, last_name, gender, level

* <b>songs</b> - songs in music database
song_id, title, artist_id, year, duration

* <b>artists</b> - artists in music database
artist_id, name, location, latitude, longitude

* <b>time</b> - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday


<h3>Purpose of Schema</h3>
The purpose of previously depicted schema is to have each entry as normalized as possible (3 normal form)
in order to make writting as easy as possible (reduce redundancy and increase integrity) and
to make analyzing tasks possible with introducing a fact table to easy execute queries without JOINS being needed.

<h3>How to</h3>
To run this project e.g. spin up Redshift Cluster, populate Warehouse and analyze the results of the etl pipeline do:
* Fill ``dwh.cfg`` with your AWS Key and Secret (do not share this)
* If you run the code locally, make sure your environment has all the necessary libs installed e.g. Pandas, JSON, boto3, configparser
* Run all the cells in ``create_cluster.ipynb`` to spin it up on AWS (will take a couple of minutes)
* Run ``create_tables.py`` to create table schema on warehouse using queries in ``sql_queries.py``
* Run ``etl.py`` to populate tables from the staging tables 
* Run cells in ``analyze_cluster.ipynb`` to have a short summary of results of etl pipeline and table schema creation
* make sure to delete Redshift cluster after you are done e.g. by running ``shutdown_cluster.ipynb`` 