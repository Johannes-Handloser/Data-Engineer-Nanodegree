# Udacity DEND: Project 4 Data Lakes with Spark

## General

## Project Dataset
* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

structured like: 
Song Data
* song_data/A/B/C/TRABCEI128F424C983.json
* song_data/A/A/B/TRAABJL12903CDCF1A.json

Log Data
* log_data/2018/11/2018-11-12-events.json
* log_data/2018/11/2018-11-13-events.json

with an entry of song data looking like:
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## Database Schema

**Fact Table**
songplays - records in log data associated with song plays i.e. records with page NextSong
* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
users - users in the app
* user_id, first_name, last_name, gender, level

songs - songs in music database
* song_id, title, artist_id, year, duration

artists - artists in music database
* artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
* start_time, hour, day, week, month, year, weekday


## How To

In order to run the project, you need to fulfill the following prerequisites
* Python compiler & interpreter installed 
* AWS Account
* All needed dependencies installed via Pip 

Enter your AWS Credentials in **dl.cfg**
```console
[AWS]
AWS_ACCESS_KEY_ID = <your aws key>
AWS_SECRET_ACCESS_KEY = <your aws secret>
```

Run the etl pipeline in **etl.py**
```console
python etl.py
```
This will trigger the etl pipeline to populate the S3 bucket in **s3a://udacity-jh-dend/** with the tables defined in the schema above. As datasource, the S3 Bucket in **s3a://udacity-dend/** is used with the logs and song jsons being described in 'Project Dataset'

All other files and folders
* query_tests.ipynb
* testdata
* data

where used for testing purposes with snippets of data.

