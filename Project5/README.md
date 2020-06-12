# Project 5 - Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.


### Prerequisites


Tables must be created in Redshift beforehand to successful execute the DAG workflow. The create tables statements can be found in:

`airflow/create_tables.sql`

Connection credentials must be stored on Airflow instance for general AWS and Redshift

## Data Sources

Data resides in two directories that contain files in JSON format:

1. Log data: s3://udacity-dend/log_data
2. Song data: s3://udacity-dend/song_data


## Scripts Usage

DAG & DDL
* `create_tables.sql` - Contains the DDL for all tables which should exist on Redshift DB
* `etl_dag.py` - Overall Airflow DAG for ETL Pipeline

Operators
* `stage_redshift.py` - Operator to read files from S3 and load into Redshift staging tables
* `load_fact.py` - Operator to create the fact table in Redshift
* `load_dimension.py` - Operator to create the dimension tables in Redshift
* `data_quality.py` - Operator for data quality checking of created tables

## Built Dependencies

* [Python 3.6.2](https://www.python.org/downloads/release/python-363/) 
* [Apache Airflow 1.10.2](https://airflow.apache.org/)  

