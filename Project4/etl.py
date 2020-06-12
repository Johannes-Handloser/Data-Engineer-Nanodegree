import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id, udf, col, to_date
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create a Spark Session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Function to transform song_data on S3 into song and artist tables
    
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    songs_table.createOrReplaceTempView("songs_table")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data+"songs_table/songs.parquet")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_latitude", "artist_location", "artist_longitude", "artist_name").withColumnRenamed("artist_name", "name") \
    .withColumnRenamed("artist_latitude", "latitude") \
    .withColumnRenamed("artist_longitude", "longitude") \
    .withColumnRenamed("artist_location", "location") \
    .dropDuplicates()
    artists_table.createOrReplaceTempView("artists_table")
        
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists_table/artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Load data from log_data dataset and extract columns for user and time tables. 
    Data is written to parquet files and stored on S3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users_table/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select("start_time") \
                        .withColumn("hour", hour("start_time")) \
                        .withColumn("day", dayofmonth("start_time")) \
                        .withColumn("week", weekofyear("start_time")) \
                        .withColumn("month", month("start_time")) \
                        .withColumn("year", year("start_time")) \
                        .withColumn("weekday", dayofweek("start_time")) \
                        .dropDuplicates()
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"time_table/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = df.join(song_df, song_df.artist_name == df.artist, "inner")
    songplays_table = joined_df.select(
    col("start_time"),
    col("userId").alias("user_id"),
    col("level"),
    col("song_id"),
    col("artist_id"),
    col("sessionId").alias("session_id"),
    col("location"), 
    year("start_time").alias("year"),
    month("start_time").alias("month"),
    col("userAgent").alias("user_agent"))\
    .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"songplays/songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-jh-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
