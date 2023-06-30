# Introduction and Purpose

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

First we'll define and create new fact and dimension tables according to the star schema, which makes it easy and efficient to execute analytical join queries.

Then we'll build an ETL pipeline to first extract the data from S3, processes it using Spark. And then load the data back into S3 as a set of dimensional tables.

# Database schema design and ETL pipeline

'songplays' is our Fact Table. This contains records in log data associated with song plays. Data is filtered to contain only information associated with 'NextSong' page. We've implemented auto incremental Identity key value for 'songplay_id'. The processed data will be partitioned on 'artist_id' and 'song_id' columns as they will be most commonly used in WHERE and JOIN queries.

'users' is our Dimension Table. This contains records of all the users in the app. It contans name of the user, gender and level of subscription of the app. The processed data will be partitioned on 'gender' & 'level' columns.

'songs' is our Dimension Table. This contains records of all songs in the music database. It contains song title, year of release and duration. The processed data will be partitioned on 'year' & 'artist_id' columns.

'artists' is our Dimension Table. This contains records of all artists in music database. It contains name of the artist and location details.

'time' is our Dimension Table. This contains records of all timestamps of records in songplays broken down into specific units. Date/time data is broken down into hour, day, week, month, year, weekday columns. The processed data will be partitioned on 'year' & 'month' columns.

# How to run python scripts and description of the files

'etl.py' is where we load data from S3, process the data into analytics tables using Spark; and then load them back into S3.

'dl.cfg' is where we store credentials of aws account where spark cluster is running.