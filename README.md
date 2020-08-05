# Project: Data Lake

## Introduction 
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I was tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

I will be able test my database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.

Project Description
In this project, I will apply what I have learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. I will deploy this Spark process on a cluster using AWS.

## Schema
I used a star schema that is optimized for queries on song play analysis.  
The Fact Table is songplay_table it has six of the eight columns are from staging_events data (start_time, userId, level, sessionId, location, userAgent) and two from staging_songs (song_id, artist_id).  
The Dimension Tables are as follows:  
users - _users in the app_ (user_id, first_name, last_name, gender, level)   
songs - _songs in the music database_ (song_id, title, artist_id, year, duration)  
artists - _artist in the music database_ (artist_id, name, location, lattitude, longitude)  
time - _timestamps of records in songplays broken into units in sperate columns_ (start_time, hour, day, week, month, year, weekday)  


![Project 3 Schema](https://github.com/seisolo76/UDACITY-Project-3-Data-Warehouse/blob/master/Project%203%20schema.png)

## Project Files
etl.py - main program to extract the data from s3 bucket. Transform from two data sources to five tables. write it back to s3 bucket in 5 directories with parquet files.
dl.cfg - location of AWS Access key id and secret access key 
README.MD - This file

## Execute Script
execute the ETL process using the python program named etl.py
