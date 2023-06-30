import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
import pyspark.sql.functions as F
from pyspark.sql.types import StructField,IntegerType, StructType,StringType,DecimalType,TimestampType,LongType
from pyspark.sql.functions import from_unixtime,hour,dayofmonth,weekofyear,month,year,dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Process song data from given input_path and store it at output_data as parquet files.

    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    ## STAGING SONGS TABLE

    # staging songs table schema
    staging_songs_table_schema = StructType([
        StructField("artist_id",        StringType(),      True),
        StructField("artist_latitude",  DecimalType(10,8), True),
        StructField("artist_longitude", DecimalType(11,8), True),
        StructField("artist_location",  StringType(),      True),
        StructField("artist_name",      StringType(),      True),
        StructField("song_id",          StringType(),      True),
        StructField("title",            StringType(),      True),
        StructField("duration",         DecimalType(9,5),  True),
        StructField("year",             IntegerType(),     True)
    ])

    # read song data files
    staging_songs_table = spark.read.json(song_data, schema=staging_songs_table_schema)

    ## SONGS TABLE

    # songs table schema
    songs_table_schema = StructType([
        StructField("song_id",   StringType(),     False),
        StructField("title",     StringType(),     False),
        StructField("artist_id", StringType(),     True),
        StructField("year",      IntegerType(),    True),
        StructField("duration",  DecimalType(9,5), True)
    ])

    # extract columns to create songs table
    songs_table = spark.createDataFrame(staging_songs_table \
                       .select("song_id","title","artist_id","year","duration").rdd, \
                       schema=songs_table_schema)
    
    # write songs table to parquet files partitioned by year and artist_id
    songs_table.write.partitionBy("year", "artist_id") \
               .parquet(output_data + "songs_table/")
    
    ## ARTIST TABLE

    # artist table schema
    artists_table_schema = StructType([
        StructField("artist_id",        StringType(),      False),
        StructField("artist_name",      StringType(),      False),
        StructField("artist_location",  StringType(),      True),
        StructField("artist_latitude",  DecimalType(10,8), True),
        StructField("artist_longitude", DecimalType(11,8), True)
    ])

    # extract columns to create artists table
    artists_table = spark.createDataFrame(staging_songs_table \
                         .select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").rdd, \
                         schema=artists_table_schema)
    
    # rename columns
    artists_table = artists_table.withColumnRenamed("artist_name","name") \
                                 .withColumnRenamed("artist_location","location") \
                                 .withColumnRenamed("artist_latitude","latitude") \
                                 .withColumnRenamed("artist_longitude","longitude")
    
    # write artists table to parquet files
    artists_table.write \
                 .parquet(output_data + "artists_table/")


def process_log_data(spark, input_data, output_data):
    """Process log data from given input_path and store it at output_data as parquet files.

    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    ## STAGING LOG TABLE

    # staging log table schema
    ''''''
    staging_log_table_schema = StructType([
        StructField("artist",    StringType(),     True),
        StructField("firstName", StringType(),     True),
        StructField("lastName",  StringType(),     True),
        StructField("gender",    StringType(),     True),
        StructField("length",    DecimalType(9,5), True),
        StructField("level",     StringType(),     True),
        StructField("location",  StringType(),     True),
        StructField("page",      StringType(),     True),
        StructField("sessionId", IntegerType(),    True),
        StructField("song",      StringType(),     True),
        StructField("status",    IntegerType(),    True),
        StructField("ts",        TimestampType(),  True),   
        StructField("userAgent", StringType(),     True),
        StructField("userId",    StringType(),     True)  
    ])

    # read log data files
    staging_log_table = spark.read.json(log_data, schema=staging_log_table_schema)

    # drop null/blank and duplicate values
    staging_log_table = staging_log_table.filter((staging_log_table['userId'].isNotNull()) & (staging_log_table['userId'] != "")==True)
    staging_log_table = staging_log_table.dropDuplicates(["userId"])

    # convert datatype of userId column to integer from string | userid is in string from source
    staging_log_table =staging_log_table.withColumn("userId", staging_log_table["userId"].cast(IntegerType()))

    # convert unix epoch time to readable date & time value
    staging_log_table = staging_log_table.withColumn("start_time",from_unixtime(F.col("ts").cast(LongType())/1000))

    ## convert datatype of start_time from string to timestamp
    staging_log_table = staging_log_table.withColumn("start_time",F.col("start_time").cast(TimestampType()))

    # create new columns from start_time column
    staging_log_table = staging_log_table.withColumn("hour",    hour(F.col("start_time"))) \
                                         .withColumn("day",     dayofmonth(F.col("start_time"))) \
                                         .withColumn("week",    weekofyear(F.col("start_time"))) \
                                         .withColumn("month",   month(F.col("start_time"))) \
                                         .withColumn("year",    year(F.col("start_time"))) \
                                         .withColumn("weekday", dayofweek(F.col("start_time")))
    
    ## USERS TABLE
    
    # users table schema
    users_table_schema = StructType([
        StructField("userId",    IntegerType(), False),
        StructField("firstName", StringType(),  True),
        StructField("lastName",  StringType(),  True),
        StructField("gender",    StringType(),  True),
        StructField("level",     StringType(),  True)
    ])

    # extract columns to create users table    
    users_table = spark.createDataFrame(staging_log_table \
                       .select("userId","firstName","lastName","gender","level").rdd, \
                       schema=users_table_schema)
    
    # rename columns
    users_table = users_table.withColumnRenamed("userId","user_id") \
                                 .withColumnRenamed("firstName","first_name") \
                                 .withColumnRenamed("lastName","last_name")

    # write users table to parquet files
    users_table.write.partitionBy("gender", "level") \
               .parquet(output_data + "users_table/")
    
    ## TIME TABLE
    
    # time table schema
    time_table_schema = StructType([
        StructField("start_time", TimestampType(), False),
        StructField("hour",       IntegerType(),   True),
        StructField("day",        IntegerType(),   True),
        StructField("week",       IntegerType(),   True),
        StructField("month",      IntegerType(),   True),
        StructField("year",       IntegerType(),   True),
        StructField("weekday",    IntegerType(),   True)
    ])

    # extract columns to create time table
    time_table = spark.createDataFrame(staging_log_table \
                      .select("start_time","hour","day","week","month","year","weekday").rdd, \
                      schema=time_table_schema)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month") \
              .parquet(output_data + "time_table/")
    
    ## SONGPLAYS TABLE
    
    # song data files path
    song_data = input_data + "song_data/*/*/*/*.json"

    # songs table schema
    songs_table_schema = StructType([
        StructField("song_id",   StringType(),     False),
        StructField("title",     StringType(),     False),
        StructField("artist_id", StringType(),     True),
        StructField("year",      IntegerType(),    True),
        StructField("duration",  DecimalType(9,5), True)
    ])
    
    # read song data files for songplays table
    songs_table = spark.read.json(song_data, schema=songs_table_schema)
    
    # join staging log table with songs table to add song_id and artist_id
    staging_log_table = staging_log_table.join(songs_table,staging_log_table["song"]==songs_table["title"],"inner") \
                                         .select(staging_log_table['*'], "song_id", "artist_id")
         
    # songplays table schema
    songplays_table_schema = StructType([
        StructField("start_time", TimestampType(), False),
        StructField("userId",     IntegerType(),   True),
        StructField("level",      StringType(),    True),
        StructField("song_id",    StringType(),    True),
        StructField("artist_id",  StringType(),    True),
        StructField("sessionId",  IntegerType(),   True),
        StructField("location",   StringType(),    True),
        StructField("userAgent",  StringType(),    True),
        StructField("year",       IntegerType(),   True),
        StructField("month",      IntegerType(),   True)
    ])

    # extract columns to create songplays table
    songplays_table = spark.createDataFrame(staging_log_table.filter(staging_log_table["page"] == "NextSong") \
                           .select("start_time","userId","level","song_id","artist_id","sessionId","location","userAgent","year","month").rdd, \
                           schema=songplays_table_schema)
    
    # add auto-incremental songplay_id column
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())

    # rearrange songplay_id column
    songplays_table = songplays_table.select("songplay_id","start_time","userId","level","song_id","artist_id", \
                                             "sessionId","location","userAgent","year","month")

    # rename columns
    songplays_table = songplays_table.withColumnRenamed("userId","user_id") \
                                     .withColumnRenamed("sessionId","session_id") \
                                     .withColumnRenamed("userAgent","user_agent")    

    # write songplays table to parquet files partitioned by artist_id and song_id
    songplays_table.write.partitionBy("year","month") \
                   .parquet(output_data + "songplays_table/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://emr-spark11/proj_parquet_write/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
