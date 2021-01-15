# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 17:57:48 2021

@author: jathurshan GNANASEELAN
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

#1) Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("walmart")\
                    .getOrCreate()

#2) important les données de Walmart stock
                    
dt=spark.read\
        .option("header",True)\
        .csv("Input/walmart_stock.csv")
#les données represente les cours de l'action de Walmard 
dt.show(2)

#3Columns Names
dt.column
#Colnames of    
#Date|Open| High|Low|Close|Volume|Adj Close|

#4) Print Schema de la base de donnée
dt.printSchema()
#nous remarquons que nous avons des donnée de type "String"

#5) Calculer du ratio HV_RAtion
dt1=dt.withColumn("HV",col("High")/col("Volume"))

#6) Peak High in Price.
dt1.orderBy(col("High").desc()).select(col("Date")).head()
# PeakHigh pRice with sql

dt1.createOrReplaceTempView("Stock")
#on transforme la DataTable en table
spark.sql("""SELECT Date FROM Stock ORDER BY High""").show(1)
#solution

#8) Calcul de la moyenne
dt1.select("Close").summary("mean").show()
# Méthode Sql
spark.sql("""SELECT AVG(Close) as AVG_Close  FROM Stock """).show()

#9) Min et Max de Close 
dt1.select("Volume").summary("min","max").show()
# Méthode Sql
spark.sql("""SELECT Min(Volume) as Min_Close, Max(Volume) as Max_Close  FROM Stock """)\
    .show()

#10)How many was the Close lower than 60 dollars?
dt1.filter(dt.Close<=60).count()
#methode SQL
spark.sql("""SELECT Count (*)  as Compte FROM Stock WHERE( Close <= 60)""")\
    .show()

#11) Max Per Year
dt1.groupBy(F.year("Date")).agg({'High': 'max'}).sort(F.year("Date"))\
   .show()
#methode SQL
spark.sql("""SELECT  Year(Date) as Annee, max(High) as Max_High FROM Stock \
          Group By Annee Order by Annee  """).show()


#12) Average per Month
#12) average Close for each Calendar Month

dt.groupBy(F.last_day('date'))\
  .agg({'Close': 'mean'})\
  .sort(F.last_day('date'))\
  .show()
#methode SQL

##Arreter Spark
spark.stop()