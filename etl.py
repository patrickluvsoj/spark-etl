from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


os.environ['AWS_ACCESS_KEY_ID']= #empty
os.environ['AWS_SECRET_ACCESS_KEY']= #empty

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreates()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songdata")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songdata
        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs_table'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, artist_name, artist_location, 
                        artist_latitude, artist_longitude
        FROM songdata
        """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # extract columns for users table    
    df.createOrReplaceTempView("logdata")

    users_table = spark.sql("""
        SELECT DISTINCT userId, firstName, 
                        lastName, gender, level
        FROM logdata
        WHERE page = 'NextSong'
        """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users_table'), 'overwrite')

    # create timestamp column from original timestamp column
    df.withColumn('ts', F.to_timestamp(F.col('ts')/1000))
    
    # extract columns to create time table
    df.createOrReplaceTempView("logdata")

    time_table = spark.sql("""
        SELECT DISTINCT ts as start_time, hour(ts) as hour, 
                        day(ts) as day, month(ts) as month, year(ts) as year, 
                        dayofweek(ts) as weekday
        FROM logdata
        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').\
        parquet(os.path.join(output_data, 'time_table'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    
    # create TempView for query
    song_df.createOrReplaceTempView("songdata")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT l.ts as start_time, year(l.ts) as year, month(l.ts) as month,
           l.userId, l.level, s.song_id, s.artist_id, 
           l.sessionId, l.location, l.userAgent
        FROM logdata l
        LEFT JOIN songdata s
        ON l.artist = s.artist_name AND l.song = s.title
        """)

    # Add songplay_id column
    songplays_table = songplays_table.withColumn('songplay_id' , F.monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').\
        parquet(os.path.join(output_data, 'songplay_table'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3a://patrickudacity-songplays/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
