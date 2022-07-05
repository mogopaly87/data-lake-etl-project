from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as t
import os


song_data_source = os.environ["INPUT_SONG_DATA"]
log_data_source = os.environ["INPUT_LOG_DATA"]
output = os.environ["OUTPUT_DATA_DIR"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark):

    # read song data file
    df_song_data = spark.read.json(song_data_source)

    # extract columns to create songs table
    df_song_data.createOrReplaceTempView("songs_table_view")
    songs_table = spark.sql(
        "SELECT artist_name, song_id, title, artist_id, year, duration FROM songs_table_view"
    )

    # create timestamp attached to each song parquet file
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    song_table_path = "songs_table" + ".parquet" + "_" + now

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output + song_table_path)

    # extract columns to create artists table
    df_song_data.createOrReplaceTempView("artist_table_view")
    artist_table = spark.sql(
        "\
        SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude\
        FROM artist_table_view "
    )

    # create timestamp attached to each artist parquet file
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    artist_table_path = "artist_table" + ".parquet" + "_" + now

    # write artists table to parquet files
    artist_table.write.partitionBy("artist_id").parquet(output + artist_table_path)


def process_log_data(spark):

    # read log data file
    df_log_data = spark.read.json(log_data_source)

    # filter by actions for song plays
    filtered_ld_df = df_log_data.filter(df_log_data.page == "NextSong")

    # extract columns for users table
    filtered_ld_df.createOrReplaceTempView("users_table_view")
    user_table = spark.sql(
        "SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level\
            FROM users_table_view"
    )

    # create timestamp attached to each users parquet file
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    users_table_path = "users_table" + ".parquet" + "_" + now

    # write users table to parquet files
    user_table.write.partitionBy("user_id").parquet(output + users_table_path)

    # create timestamp column from original timestamp column
    filtered_ld_df = filtered_ld_df.withColumn("timestamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    filtered_ld_df = filtered_ld_df.withColumn("datetime", get_datetime("ts"))

    # extract columns to create time table
    filtered_ld_df.createOrReplaceTempView("time_table_view")
    time_table = spark.sql(
        """SELECT DISTINCT datetime AS start_time, HOUR(timestamp) AS hour,
                DAY(timestamp) AS day, WEEKOFYEAR(timestamp) AS week,
                MONTH(timestamp) AS month, YEAR(timestamp) AS year,
                DAYOFWEEK(timestamp) AS weekday
            FROM time_table_view"""
    )

    # create timestamp attached to each time table parquet file
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    time_table_path = "time_table" + ".parquet" + "_" + now

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year").parquet(output + time_table_path)

    # read in song data to use for songplays table
    df_song_data = spark.read.json(song_data_source)

    # extract columns from joined song and log datasets to create songplays table
    joined_ld_sd_df = filtered_ld_df.join(
        df_song_data,
        (filtered_ld_df.artist == df_song_data.artist_name)
        & (filtered_ld_df.song == df_song_data.title),
    )

    # write songplays table to parquet files partitioned by year and month
    joined_ld_sd_df = joined_ld_sd_df.withColumn(
        "songplay_id", F.monotonically_increasing_id()
    )
    joined_ld_sd_df.createOrReplaceTempView("songplays_table_view")

    songplays_table = spark.sql(
        """
            SELECT  songplay_id AS songplay_id,
                    timestamp   AS start_time,
                    userId      AS user_id,
                    level       AS level,
                    song_id     AS song_id,
                    artist_id   AS artist_id,
                    sessionId   AS session_id,
                    location    AS location,
                    userAgent   AS user_agent
            FROM songplays_table_view
            ORDER BY (user_id, session_id)
        """
    )

    # create timestamp attached to each songplay table parquet file
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    songplay_table_path = "songplay_table" + ".parquet" + "_" + now

    songplays_table.write.parquet(output + songplay_table_path)


@F.udf(t.TimestampType())
def get_timestamp(ts):
    return datetime.fromtimestamp(ts / 1000.0)


@F.udf(t.StringType())
def get_datetime(ts):
    return datetime.fromtimestamp(ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S")


def main():
    spark = create_spark_session()

    process_song_data(spark)
    process_log_data(spark)


if __name__ == "__main__":
    main()
