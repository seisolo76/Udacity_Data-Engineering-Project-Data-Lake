import configparser
from datetime import datetime
import os
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Lng, TimestampType as TST
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,  to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS_ID', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS_SEC', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Creates a spark session
       with spark.jars and apache.hadoop packages
    
    Parameters:No arguments
    
    Returns: spark session named "spark"
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """import Song dataset extract columns and create songs and artist tables
    write those tables to parquet files
    
    Parameters:
    spark: name of spark session
    input_data: location of the source data s3 bucket 
    output_data: location of the destination data s3 bucket
    
    Returns:
    writes songs table in parquet to output_data location + songs
    writes artist_table in parquet to output_dat location + artists
    
    """
    
    # Setting up the JSON table structure for the Song dataset
    song_dataset_schema= R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Str()),
        ])
    
    """get filepath to song data file 
    use "song_data/*/*/*/*.json" for full dataset
    use "song_data/A/B/C/TRABCEI128F424C983.json" to pull a single record

    """
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file with dataset_schema
    df = spark.read.json(song_data, schema=song_dataset_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')
    
    # drop duplicate rows in songs table
    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('append').partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # drop duplicate rows in artists table
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('append').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """import log dataset extract columns create users, time, and songplays table
    
    Parameters:
    spark: name of spark session
    input_data: location of the source data s3 bucket 
    output_data: location of the destination data s3 bucket
    
    Returns:
    writes users table in parquet to output_data location + users
    writes time_table in parquet to output_data location + time
    writes songplays table in parquest to output_data location + songplays    
    
    """
    
    """Setting up the JSON table structure for the log dataset"""
    log_dataset_schema= R([
       Fld("artist", Str()),
       Fld("auth", Str()),
       Fld("firstName", Str()),
       Fld("gender", Str()),
       Fld("iteminSession", Lng()),
       Fld("lastName", Str()),
       Fld("length", Dbl()),
       Fld("level", Str()),
       Fld("location", Str()),
       Fld("method", Str()),
       Fld("page", Str()),
       Fld("registration", Dbl()),
       Fld("sessionId", Lng()),
       Fld("song", Str()),
       Fld("status", Lng()),
       Fld("ts", Lng()),
       Fld("userAgent", Str()),
       Fld("userId", Str()),  
   ])
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data, schema = log_dataset_schema)
    
    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"), "gender", "level") 
    
    # drop duplicate rows in users table 
    users_table = users_table.dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('append').parquet(output_data+"users")

    # create timestamp column from original timestamp column
    dfTimestamp = df.withColumn("start_time", to_timestamp(col("ts") /1000 ))
    dfTimestamp = dfTimestamp \
    .withColumn("hour", hour("start_time")) \
    .withColumn("day", dayofmonth("start_time")) \
    .withColumn("week", weekofyear("start_time")) \
    .withColumn("month", month("start_time")) \
    .withColumn("year", year("start_time")) \
    .withColumn("weekday", date_format("start_time", 'E'))
    
     
    # extract columns to create time table
    time_table = dfTimestamp.select(col("start_time"), \
                                    col("hour"), \
                                    col("day"), \
                                    col("week"), \
                                    col("month"), \
                                    col("year"), \
                                    col("weekday"))
    
    # drop duplicate rows in time table 
    time_table = time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('append').partitionBy("year","month").parquet(output_data+"time")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.artist_name==df.artist). \
        withColumn("songplay_id", monotonically_increasing_id()). \
        withColumn('start_time', to_timestamp(col("ts") /1000 )). \
        select("songplay_id",
        "start_time",                      
        col("userId").alias("user_id"), 
        "level", 
        "song_id", 
        "artist_id", 
        col("sessionId").alias("session_id"), 
        col("artist_location").alias("location"), 
        "userAgent", 
        month(col("start_time")).alias("month"), 
        year(col("start_time")).alias("year")) 

    # drop duplicate rows in songplays table 
    songplays_table = songplays_table.dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('append').partitionBy('year', 'month').parquet(output_data + "songplays")


def main():
    """main function executes create_spark_session, process_song_data function, and
    process_log_data function with input_data and output_data defined.
    
    Parameters: No arguments
    
    Returns: No Returns
    
   
    """
    spark = create_spark_session()
    # input_data defines the location of the source s3 bucket
    input_data = "s3a://udacity-dend/"
    # output_data defines the location of the destination s3 bucket
    
    output_data = "s3a://udacity-dend"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
