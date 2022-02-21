from argparse import ArgumentParser
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


class CalculateSessionImpl:

    def __init__(self, spark_session, file):
        self.spark = spark_session
        self.file = file

    def calculate(self):
        df = self.spark.read.csv(self.file, header=True, sep=',', inferSchema=True)
        # convert "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'" to timestamp, using a new column and drop existing column
        df_stream = df.withColumn('Action_time_parsed',
                                  to_timestamp(col('Action_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                                  ).drop("Action_time")
        window_spec = Window.partitionBy("User_ID").orderBy("Action_time_parsed")
        # for each User_Id, find out those time difference less than 5 minutes : 5*60 seconds
        df_stream = df_stream.withColumn("time_diff",
                                         (f.unix_timestamp("Action_time_parsed") -
                                          f.unix_timestamp(f.lag(f.col("Action_time_parsed"), 1).over(window_spec))
                                          ) / (5 * 60)).na.fill(0)
        # create a new column that time difference exceeds five minutes
        df_stream = df_stream.withColumn("exceed_five_minutes", f.when(f.col("time_diff") > 1, 1).otherwise(0))
        # according to the value in column: exceed_five_minutes, clarify the same records as temporary session_number
        df_stream = df_stream.withColumn("session_number_temp", f.sum(f.col("exceed_five_minutes")).over(window_spec))

        # create a new window to process newly added column: session_number_temp and Action_time_parsed.
        new_window = Window.partitionBy("User_ID", "session_number_temp").orderBy("Action_time_parsed")
        new_spec = new_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

        hours = (f.unix_timestamp("Action_time_parsed") -
                 f.unix_timestamp(f.lag(f.col("Action_time_parsed"), 1).over(new_window)))
        # for each session_number, calculate the session duration to be seconds
        df_stream = df_stream.withColumn("session_duration_seconds", hours).na.fill(0)
        # for each session_number, calculate the total session duration time as seconds
        df_stream = df_stream.withColumn("session_duration_sum",
                                         f.sum(f.col("session_duration_seconds")).over(new_spec))
        # group by User_Id and session_number_temp, and count total URL & unique url
        df_stream = df_stream.groupBy('User_ID', 'session_number_temp') \
            .agg(f.max('session_duration_sum').alias("session_duration_unfixed"),
                 count('URL').alias("count_total_url"),
                 countDistinct('URL').alias("count_unique_url"))
        # as for session_duration, the last  60 seconds should be active, so sum 60seconds up
        # as for session_number should star from 1
        df_stream = df_stream.withColumn("session_number", df_stream["session_number_temp"] + 1) \
            .withColumn("session_duration", df_stream["session_duration_unfixed"] + 60)
        # select "User_ID", "session_number", "session_duration", "count_total_url", "count_unique_url"
        df_stream = df_stream.select("User_ID",
                                     "session_number",
                                     "session_duration",
                                     "count_total_url",
                                     "count_unique_url")
        df_stream.show(df_stream.count(), truncate=False)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Read user session CSV data and calculate each user_id's active session duration") \
        .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")

    parser = ArgumentParser(description='User session CSV data, ex: example_data_2019_v1.csv')
    parser.add_argument('-f', metavar='CSV input file', help="CSV file, ex: example_data_2019_v1.csv",
                        default="/Users/xiaoshuang.xu/Documents/J_プログラミングテスト/example_data_2019_v1.csv")
    args = parser.parse_args()
    file_str = args.f
    calculate_session_service = CalculateSessionImpl(spark, file_str)
    calculate_session_service.calculate()
