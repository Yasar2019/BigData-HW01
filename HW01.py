from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from operator import add
import re

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
    # Initialize Spark
    conf = SparkConf().setAppName("WordCountTitle") \
                      .set("spark.driver.extraJavaOptions", r"-Dhadoop.home.dir=C:\Users\Asus\Downloads\spark-3.5.0-bin-hadoop3\spark-3.5.0-bin-hadoop3\bin")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # Read the data
    data = spark.read.csv("spacenews-202309.csv",
                          header=True, inferSchema=True)

    # Extract titles and count words
    titles = data.rdd.map(lambda row: row['title'])
    total_word_counts = titles.flatMap(word_count_per_title).reduceByKey(
        add).sortBy(lambda x: x[1], ascending=False)

    # Convert total word counts to DataFrame and save to CSV
    total_word_counts_df = total_word_counts.toDF(["word", "count"])
    total_word_counts_df.write.csv("output/total_word_counts.csv", header=True)

    # Extract date and title and count words per date
    date_titles = data.rdd.map(lambda row: (row['date'], row['title']))
    per_date_word_counts_rdd = date_titles.flatMap(word_count_per_date).map(
        lambda x: (x, 1)).reduceByKey(add).map(lambda x: (x[0][0], x[0][1], x[1]))

    # Convert per date word counts to DataFrame and save to CSV
    per_date_word_counts_df = spark.createDataFrame(
        per_date_word_counts_rdd, ["date", "word", "count"])
    per_date_word_counts_df.write.partitionBy("date").csv(
        "output/per_date_word_counts", header=True)

    # Stop the SparkContext
    sc.stop()
