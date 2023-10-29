from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from operator import add
import re
import pandas as pd

import os
print(os.environ['HADOOP_HOME'])


def word_count_per_title(title):
    words = re.findall(r'\w+', title)
    return [(word.lower(), 1) for word in words]


def word_count_per_date(date_title):
    date, title = date_title
    words = re.findall(r'\w+', title)
    return [(date, word.lower()) for word in words]


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PowerConsumptionStats") \
        .master("local") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config("spark.hadoop.home.dir", "C:/Users/Asus/Downloads/spark-3.5.0-bin-hadoop3/spark-3.5.0-bin-hadoop3/bin") \
        .getOrCreate()

    sc = spark.sparkContext

    # Read the data
    # data = spark.read.csv("spacenews-202309.csv", header=True, inferSchema=True)
    data = spark.read.option("header", "true").option("quote", "\"").option(
        "escape", "\"").option("multiline", "true").csv('spacenews-202309.csv')
    # Extract titles and count words
    titles = data.rdd.map(lambda row: row['title'])
    total_word_counts = titles.flatMap(word_count_per_title).reduceByKey(
        add).sortBy(lambda x: x[1], ascending=False)

    # Convert total word counts to DataFrame and save to CSV
    total_word_counts_df = total_word_counts.toDF(["word", "count"]).toPandas()
    total_word_counts_df.to_csv("output/total_word_counts.csv", header=True)

    # Extract date and title and count words per date
    # date_titles = data.rdd.map(lambda row: (row['date'], row['title']))
    # per_date_word_counts_rdd = date_titles.flatMap(word_count_per_date).map(
    #    lambda x: (x, 1)).reduceByKey(add).map(lambda x: (x[0][0], x[0][1], x[1]))

    # Convert per date word counts to DataFrame and save to CSV
    # per_date_word_counts_pd_df = spark.createDataFrame(
    #    per_date_word_counts_rdd, ["date", "word", "count"]).toPandas()
    # per_date_word_counts_df.write.partitionBy("date").csv("output/per_date_word_counts", header=True)
    # for date, group in per_date_word_counts_pd_df.groupby('date'):
    #   group.drop('date', axis=1).to_csv(
    #     "output/per_date.csv", index=False)

    # Extract date and word from each row
    date_word_rdd = data.rdd.flatMap(
        lambda row: [(row['date'], word) for word in row['title'].split(' ')])

    # Map each word occurrence to a 1 and reduce by key
    word_count_rdd = date_word_rdd.map(lambda x: (x, 1)).reduceByKey(add)

    # Convert the RDD to a DataFrame with columns 'date', 'word', and 'count'
    word_count_df = spark.createDataFrame(word_count_rdd.map(
        lambda x: (x[0][0], x[0][1], x[1])), ["date", "word", "count"])

    # Convert to Pandas for easier output handling
    word_count_pd = word_count_df.toPandas()

    # Save the result to a CSV
    word_count_pd.to_csv("output/per_date_word_counts.csv", index=False)
    # Stop the SparkContext
    sc.stop()
