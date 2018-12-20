#!/usr/bin/python3

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark import sql
from pyspark.sql.session import SparkSession


def clean_word(word):
    "Remove special chars in begining and end of word"

    special_chars = tuple(list(""",.;:?!"-'"""))
    while len(word) > 1 and word.startswith(special_chars):
        word = word[1:]

    while len(word) > 1 and word.endswith(special_chars):
        word = word[:-1]
    return word

def normalize_words(line):
    "function used to flatMap a line"
    line_cleaned = []

    line_tokenized = line.lower().split()
    for w in line_tokenized:
        word = clean_word(w)
        to_skip = "@" in word or "/" in word
        if not to_skip:
            line_cleaned.append(word)

    return line_cleaned

# Initialize spark app
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = sql.SQLContext(sc)
spark = SparkSession(sc)

# Textfile to RDD
filename = sys.argv[1]
book = sc.textFile(filename).flatMap(normalize_words)

# RDD to dataframe
df = sqlContext.createDataFrame(book.map(lambda x: Row(word=x)))
df.cache()
df.createOrReplaceTempView("all_words")

print('\n\nQuery result\n============\n\n')
# Longest word: brothers-in-law-for
query = """SELECT word
             FROM all_words
         ORDER BY length(word) DESC
            LIMIT 1"""
longest_word = spark.sql(query).collect()[0]
print("Longest word: ", longest_word.word)


# Most frequent 4-letter word: with
query = """SELECT word, count(word) AS freq
             FROM all_words
            WHERE length(word) = 4
         GROUP BY word
         ORDER BY freq DESC
          """
frequent_4_letter = spark.sql(query).collect()[0]
print("Most frequent 4-letter word: ", frequent_4_letter.word)

# Most frequent 15-letter word: many-fountained
query = """SELECT word, count(word) AS freq
             FROM all_words
            WHERE length(word) = 15
         GROUP BY word
         ORDER BY freq DESC
          """
frequent_15_letter = spark.sql(query).collect()[0]
print("Most frequent 15-letter word: ", frequent_15_letter.word)
print('\n\n')
