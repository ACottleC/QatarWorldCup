import sys
import null as null
from pyspark.sql import *
from pyspark.sql import functions as sf

from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("WorldCupGames") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)
    sc = spark.sparkContext
#    sc.setLogLevel("INFO") # Sets the logLevel into INFO instead of Warning

    logger.info("Starting WorldCupGames")
    # Case 1: Count the number of matches
    matches_raw_df = spark.read.options(header='True', inferSchema='True').csv('data/matches.csv')
    matches_count = matches_raw_df.count()
    print("1.Total number of Matches is: " + matches_count.__str__())

    #Case 2: Count the most repeated scores in all games
    final_score_df = matches_raw_df.withColumn("Final Score", sf.concat(sf.col("home_score"), sf.lit("-"), sf.col("away_score")))
    repeated_score_df = final_score_df.select("Final Score").groupBy("Final Score").count()\
        .withColumnRenamed("count", "Times happened").sort(sf.desc("Times happened"))
    print("2. Most repeated Score: ")
    repeated_score_df.show()

    #Case 3: Team with most Win Games
    win_teams_df = matches_raw_df.select("winning_team").filter("winning_team != 'NA'")\
        .groupby("winning_team").count()\
        .withColumnRenamed("count", "Wins").sort(sf.desc("Wins"))
    print("3. Teams that have won the most games: ")
    win_teams_df.show()

    #Case 4: Team with most Lost Games
    loss_teams_df = matches_raw_df.select("losing_team").filter("losing_team != 'NA'")\
        .groupby("losing_team").count()\
        .withColumnRenamed("count", "Losses").sort(sf.desc("Losses"))
    print("4. Teams that have lost the most games: ")
    loss_teams_df.show()

    #Case 5: Away Team Wins vs Home Team Wins
    Home_away_count_df = matches_raw_df.select("outcome")\
        .withColumn("outcome", sf.when(matches_raw_df.outcome == "H", "Home")
            .when(matches_raw_df.outcome == "A", "Away")
            .otherwise("Draw"))\
        .groupby("outcome").count()\
        .withColumnRenamed("count", "Total Outcome").sort(sf.desc("Total Outcome"))
    print("5. Home, Away and Draw Counts: ")
    Home_away_count_df.show()

    #Case 6: Average goals when playing as home and away
    print("6. Average Home Goals: " + str(matches_raw_df.select(sf.avg("home_score")).collect()[0][0])
          + " Average Away Goals: " + str(matches_raw_df.select(sf.avg("away_score")).collect()[0][0]))

    #Case 7: Cities where most matches were played
    cities_most_played_df = matches_raw_df.select ("city").groupby("city").count()\
        .withColumnRenamed("count", "Matches played").sort(sf.desc("Matches played"))
    print("7. Cities with most played matches: ")
    cities_most_played_df.show()

    #Case 8: Countries where most matches were played
    countries_most_played_df = matches_raw_df.select ("country").groupby("country").count()\
        .withColumnRenamed("count", "Matches played").sort(sf.desc("Matches played"))
    print("8. Countries with most played matches: ")
    countries_most_played_df.show()

    #Case 9: Teams that played the most matches
    home_teams_df = matches_raw_df.select("home_team")\
        .groupby("home_team").count()\
        .withColumnRenamed("count", "Matches")\
        .withColumnRenamed("home_team", "Team").sort(sf.desc("Matches"))
    away_teams_df = matches_raw_df.select("away_team")\
        .groupby("away_team").count()\
        .withColumnRenamed("count", "Matches")\
        .withColumnRenamed("away_team", "Team").sort(sf.desc("Matches"))
    total_teams_df = home_teams_df.union(away_teams_df).groupBy("Team")\
        .sum("Matches").withColumnRenamed("sum(Matches)", "Matches")\
        .orderBy(sf.desc("Matches"))
    print("9. Teams that have played the most games: ")
    total_teams_df.show()

    #Case 10: Teams that have never passed the group stage
    group_teams_df = matches_raw_df.select("home_team").filter("stage like 'Group%'")\
        .union(matches_raw_df.select("away_team").filter("stage like 'Group%'"))\
        .withColumnRenamed("Home_team", "Team").distinct()
    passed_group_teams_df = matches_raw_df.select("home_team").filter("stage not like 'Group%'")\
        .union(matches_raw_df.select("away_team").filter("stage not like 'Group%'"))\
        .withColumnRenamed("Home_team", "Team_Passed").distinct()

    never_passed_group_df = group_teams_df\
        .join(passed_group_teams_df, group_teams_df.Team == passed_group_teams_df.Team_Passed, how="left")
    print("10.Teams that have never passed the group stage : ")
    never_passed_group_df.filter("Team_passed is NULL").select("Team").withColumnRenamed("Team", "Non passed teams").show()

    #Case 11: Top Scoring Teams
    scoring_teams_df = matches_raw_df.select("home_team", "home_score")\
        .union(matches_raw_df.select("away_team", "away_score"))\
        .withColumnRenamed("home_team", "team")\
        .groupBy("team").sum("home_score").withColumnRenamed("sum(home_score)", "Total Goals")
    scoring_teams_df.orderBy(sf.desc("Total Goals")).show()


logger.info("Finished WorldCupGames")
spark.stop()

