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
    if text is None:
        return []
    text = text.lower()  
    text = re.sub(r"'s\b", '', text)
    text = re.sub(r"'\b", '', text)
    text = re.sub(r'\b"\b', '', text)
    text = re.sub(r'[^a-z\s]', '', text)
    return text.split()

def contains_space(text):
    if not text:  # Check for None or empty strings
        return False
    cleaned_words = clean_text(text)
    return 'space' in cleaned_words

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PowerConsumptionStats") \
        .master("spark://96.9.210.170:7077") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config("spark.hadoop.home.dir", "C:/Users/Asus/Downloads/spark-3.5.0-bin-hadoop3/spark-3.5.0-bin-hadoop3/bin") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.5") \
        .getOrCreate()
    sc = spark.sparkContext

    # Read the data
    data = spark.read.option("header", "true").option("quote", "\"").option(
        "escape", "\"").option("multiline", "true").csv('spacenews-202309.csv')
    
    #TASK #1 
    # Extract titles and count words
    titles = data.rdd.map(lambda row: row['title'])
    total_word_counts = titles.flatMap(word_count_per_title).reduceByKey(
        add).sortBy(lambda x: x[1], ascending=False)

    # Convert total word counts to DataFrame and save to CSV
    total_word_counts_df = total_word_counts.toDF(["word", "count"]).toPandas()
    total_word_counts_df.to_csv("output/total_word_counts.csv", header=True)

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
    
    
    # TASK#2
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
    
    
    #TASK #3 
    # Percentage of Published Articles in a Day
    date_count = data.rdd.map(lambda row: (row['date'], 1)).reduceByKey(
        lambda a, b: a + b).collect()
    total_articles = sum([count for _, count in date_count])
    date_percentage = [(date, (count / total_articles) * 100) for date, count in date_count]
    
    # Percentage of Published Articles by Authors in a Day
    date_author_count = data.rdd.map(lambda row: (
        (row['date'], row['author']), 1)).reduceByKey(lambda a, b: a + b).collect()
    date_author_percentage = [((date, author), (count / dict(date_count)[date]) * 100) for (date, author), count in date_author_count]
    
    #Convert to Pandas dataframes
    date_percentage_df = pd.DataFrame(
        date_percentage, columns=['Date', 'Percentage'])
    date_author_percentage_df = pd.DataFrame([(date, author, percentage) for (
        date, author), percentage in date_author_percentage], columns=['Date', 'Author', 'Percentage'])
    
    # Save the output to CSVs files
    date_percentage_df.to_csv("output/date_percentage.csv", index=False)
    date_author_percentage_df.to_csv("output/date_author_percentage.csv", index=False)
    
    
    #TASK #4 
    # Map operation: Check if 'Title' and 'Postexcerpt' contain the term "Space"
    def map_filter(row):
        title = (row['title'] or "").lower()  # Use an empty string if the title is None
        # Use an empty string if postexcerpt is None
        postexcerpt =(row['postexcerpt'] or "").lower()
        if "space" in title and "space" in postexcerpt:
            return row
        else:
            return None

    filtered_rdd = data.rdd.map(map_filter).filter(lambda x: x is not None)
    filtered_df = spark.createDataFrame(filtered_rdd, data.schema)
    filtered_pd = filtered_df.toPandas()
    filtered_pd.to_csv("output/space_in_title_and_postexcerpt.csv", index=False)
    
    # Stop the SparkContext
    sc.stop()
