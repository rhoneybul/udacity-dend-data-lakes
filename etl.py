import configparser
from datetime import datetime
import os
import logging
import pandas as pd
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

logging.basicConfig(level='INFO')


def create_spark_session(aws=True):
    """create spark session
    Creates, and returns a spark session object. 
    If the spark session cannot be created, the process will exit. 

    Arguments:
    aws: whether or not to use the aws hadoop config to write or read from S3 (default: True)
    """
    try:
        logging.info('Creating spark session.')
        if aws:
            spark = SparkSession \
                        .builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .appName("Sparkify Data Lakes") \
                        .getOrCreate()
        else:
            spark = SparkSession \
                        .builder \
                        .appName("Sparkify Data Lakes") \
                        .getOrCreate()
        logging.info('Spark session successfully created.')
        return spark
    except Exception as e:
        logging.error(f'Could not create spark context {e}')
        logging.fatal('Process is exiting w/ Error Code: 1')
        exit(1)

def process_song_data(spark, input_data, output_data):
    """process song data

    Arguments:
    spark: SparkSession.
    input_data: Path to the input data.
    output_data: Path to the output data.

    """
    # get filepath to song data file
    song_path = '{}/song-data/*/*/*'.format(input_data)
    
    schema = StructType([
        StructField("num_songs", IntegerType(), False),
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", FloatType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", FloatType(), False),
        StructField("year", IntegerType(), False)
    ])

    # # read song data file
    logging.info("Reading song data")
    df = spark.read.json(song_path, schema, multiLine=True)
    df.createOrReplaceTempView("songs")

    # # extract columns to create songs table
    songs_table = spark.sql('SELECT DISTINCT song_id, title, artist_id, year FROM songs')
    
    # # write songs table to parquet files partitioned by year and artist
    song_output_path = f'{output_data}/songs'
    logging.info(f'Writing song parquet files to {song_output_path}')
    songs_table.write.partitionBy('year', 'artist_id').parquet(song_output_path)

    # # extract columns to create artists table
    artists_table = spark.sql('SELECT DISTINCT artist_id, name, loation, latitude, longitude FROM songs')
    
    # # write artists table to parquet files
    artists_output_path = f'{output_data}/artists'
    logging.info(f'Writing artists parquet files to {artists_output_path}')
    artists_table.write.parquet()

def epoch_to_timestamp(e):
    """epoch_to_timestamp
    converts epoch (in ms) to a timestamp, using pandas
    Arguments:
    e: ms since epoch

    Return:
    timestamp: timestamp of the ms since the epoch given as input
    """
    return 

def epoch_to_datetime(e):
    """epoch_to_datetime
    converts epoch in ms to a datetime object
    
    Arguments:
    e: ms since epoch

    Returns:
    datetime: datetime object for the epoch input
    """
    return 
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f'{input_data}/log-data/'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    df.show()

    # create a temporary view to use spark sql
    df.createOrReplaceTempView("logs")

    # # extract columns for users table    
    users_table = spark.sql("SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level FROM logs")
    
    # # write users table to parquet files
    users_output_path=f'{output_data}/users'
    # users_table.write.parquet(users_output_path)

    # # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # # extract columns to create time table
    df.createOrReplaceTempView('logs')
    time_table = spark.sql("SELECT timestamp as start_time from logs")
    time_table = time_table.withColumn('hour', hour(time_table.start_time))
    time_table = time_table.withColumn('day', dayofmonth(time_table.start_time))
    time_table = time_table.withColumn('week', weekofyear(time_table.start_time))
    time_table = time_table.withColumn('month', month(time_table.start_time))
    time_table = time_table.withColumn('year', year(time_table.start_time))
    time_table = time_table.withColumn('weekday', date_format(time_table.start_time, 'u'))
    # time_table = df.create

    time_table.show()
    
    # # write time table to parquet files partitioned by year and month
    # time_table

    # # read in song data to use for songplays table
    # song_df = 

    # # extract columns from joined song and log datasets to create songplays table 
    # songplays_table = 

    # # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    spark = create_spark_session(aws=False)
    input_data = "./data/"
    # output_data = "s3a://udacity-data-lakes/output"
    output_data = './data/output'
    
    # try:
    #     process_song_data(spark, input_data, output_data)    
    # except Exception as e:
    #     logging.error(f'Could not process song data {e}')
    # try:
    process_log_data(spark, input_data, output_data)
    # except Exception as e:
        # logging.error(f'Could not process log data {e}')


if __name__ == "__main__":
    main()