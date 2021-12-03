'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, broadcast, quarter

import os
spark = SparkSession.builder.appName('Optimize I').getOrCreate()
answers_input_path = 'data/answers'
questions_input_path = 'data/questions'


answersDF = spark.read.option('path', answers_input_path).load()
questionsDF = spark.read.option('path', questions_input_path).load()

answersDF.repartition(8)

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
resultDF = questionsDF.join(broadcast(answers_month), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month')