from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
 
def main():
    spark = SparkSession.builder \
        .appName("NBA Scoreboard Kafka Consumer") \
        .getOrCreate()
    # Define the schema of the JSON data
    game_schema = StructType([
        StructField("gameId", StringType()),
        StructField("gameStatus", IntegerType()),
        StructField("gameStatusText", StringType()),
        StructField("gameTimeUTC", StringType()),
        StructField("homeTeam", StructType([
            StructField("teamId", StringType()),
            StructField("teamName", StringType()),
            StructField("teamCity", StringType()),
            StructField("score", IntegerType()),
            StructField("wins", IntegerType()),
            StructField("losses", IntegerType())
        ])),
        StructField("awayTeam", StructType([
            StructField("teamId", StringType()),
            StructField("teamName", StringType()),
            StructField("teamCity", StringType()),
            StructField("score", IntegerType()),
            StructField("wins", IntegerType()),
            StructField("losses", IntegerType())
        ]))
    ])
 
    scoreboard_schema = StructType([
        StructField("gameDate", StringType()),
        StructField("games", ArrayType(game_schema))
    ])
 
    full_schema = StructType([
        StructField("scoreboard", scoreboard_schema)
    ])
 
    # Read data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "nba-scoreboard-1") \
        .load()
 
    # Parse the JSON data
    json_df = df.selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), full_schema).alias("data"))
 
    # Explode the nested array to flatten the structure and select fields
    games_df = json_df.select(
        explode(col("data.scoreboard.games")).alias("game"),
        col("data.scoreboard.gameDate").alias("gameDate")
    ).select(
        "game.gameId",
        "game.gameStatus",
        "game.gameStatusText",
        "game.gameTimeUTC",
        "gameDate",
        "game.homeTeam.*",
        "game.awayTeam.*"
    )
 
    # Write to in-memory table
    query = games_df.writeStream \
                    .outputMode("append") \
                    .format("memory") \
                    .queryName("live_nba_games") \
                    .start()
 
    query.awaitTermination()
 
if __name__ == "__main__":
    main()