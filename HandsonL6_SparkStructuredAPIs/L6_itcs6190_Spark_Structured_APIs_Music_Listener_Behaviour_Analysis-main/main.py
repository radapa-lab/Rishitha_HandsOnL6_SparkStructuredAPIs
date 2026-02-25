# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs_df = spark.read.option("header", True).option("inferSchema", True).csv("listening_logs.csv")
songs_metadata_df = spark.read.option("header", True).option("inferSchema", True).csv("songs_metadata.csv")

music_df = listening_logs_df.join(songs_metadata_df, on="song_id", how="inner")

# Task 1: User Favorite Genres
genre_counts_df = music_df.groupBy("user_id", "genre").agg(count("*").alias("listen_count"))

w_fav = Window.partitionBy("user_id").orderBy(desc("listen_count"), asc("genre"))

user_favorite_genres_df = (
    genre_counts_df
    .withColumn("rn", row_number().over(w_fav))
    .filter(col("rn") == 1)
    .select("user_id", col("genre").alias("favorite_genre"), "listen_count")
)

user_favorite_genres_df.show(20, truncate=False)

# Task 2: Average Listen Time
avg_listen_time_df = (
    music_df.groupBy("user_id")
    .agg(avg("duration_sec").alias("avg_listen_time_sec"))
)

avg_listen_time_df.show(20, truncate=False)

# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
total_plays_df = music_df.groupBy("user_id").agg(count("*").alias("total_plays"))

top_genre_plays_df = user_favorite_genres_df.select(
    "user_id",
    col("listen_count").alias("top_genre_plays")
)

loyalty_scores_df = (
    total_plays_df.join(top_genre_plays_df, on="user_id", how="inner")
    .withColumn("loyalty_score", col("top_genre_plays") / col("total_plays"))
    .orderBy(desc("loyalty_score"), desc("top_genre_plays"), asc("user_id"))
    .limit(10)
)

loyalty_scores_df.show(10, truncate=False)

# Task 4: Identify users who listen between 12 AM and 5 AM
late_night_users_df = (
    music_df
    .withColumn("ts", to_timestamp(col("timestamp")))
    .filter(col("ts").isNotNull())
    .withColumn("hr", hour(col("ts")))
    .filter((col("hr") >= 0) & (col("hr") < 5))
    .groupBy("user_id").count()
    .select("user_id")
    .orderBy("user_id")
)

late_night_users_df.show(50, truncate=False)

spark.stop()