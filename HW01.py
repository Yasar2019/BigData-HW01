from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
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


def clean_text(text):
    if text is None:  # Handle None values
        return []

    text = text.lower()  # Convert to lowercase
    text = re.sub(r"'s\b", '', text)  # Remove 's
    text = re.sub(r"'\b", '', text)  # Remove trailing '
    text = re.sub(r'\b"\b', '', text)  # Remove single quotes
    text = re.sub(r'[^a-z\s]', '', text)  # Remove all non-letter characters
    return text.split()  # Split on whitespace and return the list of words


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PowerConsumptionStats") \
        .master("local") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config("spark.hadoop.home.dir", "C:/Users/Asus/Downloads/spark-3.5.0-bin-hadoop3/spark-3.5.0-bin-hadoop3/bin") \
        .getOrCreate()

    sc = spark.sparkContext


    #### TASK #1 ##################
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
    
    
    
    ############### TASK#2 ##################
    
    # Filter out rows where 'Content' is None and then extract words
    word_rdd = data.rdd.filter(lambda row: row['content'] is not None).flatMap(lambda row: clean_text(row['content']))

    # Map each word occurrence to a 1 and reduce by key
    total_word_count_rdd = word_rdd.map(lambda x: (x, 1)).reduceByKey(add)

    # Convert the RDD to a DataFrame with columns 'word' and 'count', then sort by count in descending order
    total_word_count_df = spark.createDataFrame(
        total_word_count_rdd, ["word", "count"]).orderBy('count', ascending=False)

    # Convert to Pandas for easier output handling
    total_word_count_pd = total_word_count_df.toPandas()

    # Save the result to a CSV
    total_word_count_pd.to_csv("output/total_content_word_counts.csv", index=False)
    
    
    # Filter out rows where 'Content' is None, then extract date and word from each row's 'Content'
    date_word_rdd = data.rdd.filter(lambda row: row['content'] is not None).flatMap(
        lambda row: [(row['date'], word) for word in clean_text(row['content'])])

    # Map each word occurrence to a 1 and reduce by key
    per_date_word_count_rdd = date_word_rdd.map(lambda x: (x, 1)).reduceByKey(add)

    # Convert the RDD to a DataFrame with columns 'date', 'word', and 'count', then sort by date and count
    per_date_word_count_df = spark.createDataFrame(per_date_word_count_rdd.map(lambda x: (
        x[0][0], x[0][1], x[1])), ["date", "word", "count"])
    
    # Convert 'date' column to a date type
    per_date_word_count_df = per_date_word_count_df.withColumn(
    "date", to_date(per_date_word_count_df["date"], "MMMM d, yyyy"))

    # Sort by date (chronologically) and count (descending)
    per_date_word_count_df = per_date_word_count_df.orderBy(
    ["date", "count"], ascending=[True, False])

    # Convert to Pandas for easier output handling
    per_date_word_count_pd = per_date_word_count_df.toPandas()

    # Save the result to a CSV
    per_date_word_count_pd.to_csv(
        "output/per_date_content_word_counts.csv", index=False)
    # Stop the SparkContext
    sc.stop()
