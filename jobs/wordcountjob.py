from pyspark.sql import SparkSession
from test import test
import sys

class Jobs:
    @staticmethod
    def wordcountjob(text):
        spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

        words = spark.sparkContext.parallelize(text.split(" "))

        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

        for wc in wordCounts.collect():
            print(f"{wc[0]}: {wc[1]}")

        test()

        spark.stop()

    @staticmethod
    def character_count_job(text):
        spark = SparkSession.builder.appName("PythonCharacterCount").getOrCreate()

        characters = spark.sparkContext.parallelize(text)

        characterCounts = characters.map(lambda character: (character, 1)).reduceByKey(lambda a, b: a + b)

        for cc in characterCounts.collect():
            print(f"{cc[0]}: {cc[1]}")

        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcountjob.py <function_name> <text>")
        sys.exit(-1)

    function_name = sys.argv[1]
    text = sys.argv[2]

    if function_name == "wordcountjob":
        Jobs.wordcountjob(text)
    elif function_name == "character_count_job":
        Jobs.character_count_job(text)
    else:
        print(f"Unknown function name: {function_name}")
        sys.exit(-1)