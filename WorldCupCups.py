import sys
import null as null
from pyspark.sql import *
from pyspark.sql.functions import countDistinct, desc, lit
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("WorldCupCups") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)
    sc = spark.sparkContext
#    sc.setLogLevel("INFO") # Sets the logLevel into INFO instead of Warning

    logger.info("Starting WorldCupCups")

    # Case 1: Number of Cups in total
    cups_raw_df = spark.read.options(header='True', inferSchema='True').csv('data/cups.csv')
    cups_count = cups_raw_df.count()
    print("1.Total number of Cups is: " + cups_count.__str__())

    # Case 2: Number of Cups per national team
    cup_winners_df = cups_raw_df.groupby("winner").count().withColumnRenamed("count", "Number of wins").sort(desc("Number of wins"))
    print("2.The winners are : ")
    cup_winners_df.show()

    # Case 3: Number of goals per cup
    goals_scored_df = cups_raw_df.select("host", "year", "goals_scored").sort(desc("goals_scored"))
    print("3.The Most goals scored were in : ")
    goals_scored_df.show()

    # Case 4: Number of attendance per cup

    attendance_df = cups_raw_df.select("host", "year", "attendance").sort(desc("attendance"))
    print("4.The Most attendances was in : ")
    attendance_df.show()

    # Case 5: Number of goals per match on every cup

    goals_matches_df = cups_raw_df.select("host", "year", "games", "goals_scored")
    average_goals_df = goals_matches_df.withColumn("Average goals", goals_matches_df.goals_scored / goals_matches_df.games).orderBy(desc("Average goals"))
    print("5.The average goal per match was : ")
    average_goals_df.show()

    # Case 6: Number of attendances per match on every cup

    attendance_matches_df = cups_raw_df.select("host", "year", "games", "attendance")
    average_attendance_df = attendance_matches_df.withColumn("Average attendance",
        attendance_matches_df.attendance / attendance_matches_df.games).orderBy(
        desc("Average attendance"))
    print("6.The average attendance per match was : ")
    average_attendance_df.show()

    # Case 7: Teams that Have a 1st and a 2nd place
    winners_df = cups_raw_df.select("Winner").distinct()
    seconds_df = cups_raw_df.select("Second").distinct()
    winners_seconds_df = winners_df.join(seconds_df, winners_df.Winner == seconds_df.Second, how="inner")
    print("7.Teams that have won the world cup and also had a second position : ")
    winners_seconds_df.select("Winner").withColumnRenamed("Winner", "Winner and Second").show()

    # Case 8: Teams that Have gone to the final but never won it
    final_losers_df = seconds_df.join(winners_df, seconds_df.Second == winners_df.Winner, how="left")
    print("8.Teams that have gone into finals but never won it : ")
    final_losers_df.filter("Winner is NULL").select("Second").withColumnRenamed("Second", "Final Losers").show()

    # Case 9: Best performances on all the history. Winner gets 4 points, second 3 points, Third 2 points and Fourth 1 point.
    winners_points_df = cups_raw_df.select("winner").withColumn("Points", lit(4)).groupBy("winner")\
        .sum("Points").withColumnRenamed("sum(Points)", "Total Points")\
        .withColumnRenamed("winner", "Team").orderBy(desc("Total Points"))
    seconds_points_df = cups_raw_df.select("second").withColumn("Points", lit(3)).groupBy("second") \
        .sum("Points").withColumnRenamed("sum(Points)", "Total Points")\
        .withColumnRenamed("second", "Team").orderBy(desc("Total Points"))
    thirds_points_df = cups_raw_df.select("third").withColumn("Points", lit(2)).groupBy("third") \
        .sum("Points").withColumnRenamed("sum(Points)", "Total Points")\
        .withColumnRenamed("third", "Team").orderBy(desc("Total Points"))
    fourhts_points_df = cups_raw_df.select("fourth").withColumn("Points", lit(1)).groupBy("fourth") \
        .sum("Points").withColumnRenamed("sum(Points)", "Total Points")\
        .withColumnRenamed("fourth", "Team").orderBy(desc("Total Points"))

    team_points_df = winners_points_df.union(seconds_points_df).union(thirds_points_df)\
        .union(fourhts_points_df)\
        .groupBy("Team").sum("Total Points").withColumnRenamed("sum(Total Points)", "Total Points")\
        .orderBy(desc("Total Points"))
    print("9.Best teams in history By points (winner 4 points, second 3 points, etc.) : ")
    team_points_df.show()

    logger.info("Finished WorldCupCups")
    spark.stop()