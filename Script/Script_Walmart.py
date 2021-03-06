# ==================================================================
#                            Groupe 15 :
#                          Projet DataFrame
#                        Walmart Stock Analysis
# ==================================================================

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

#1) Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("walmart")\
                    .getOrCreate()

# 2) important les données de Walmart stock

dt = spark.read \
    .option("header", True) \
    .csv("Input/walmart_stock.csv")

# les données represente les cours de l'action de Walmard
dt.show(2)

#3Columns Names
dt.columns
#Colnames of
#Date|Open| High|Low|Close|Volume|Adj Close|

#4) Print Schema de la base de donnée
dt.printSchema()
#nous remarquons que nous avons des donnée de type "String"

#5) Calculer du ratio HV_RAtion
dt1=dt.withColumn("HV",col("High")/col("Volume"))

#6) Peak High in Price.
dt1.orderBy(col("High").desc()).select(col("Date")).head()
dt1.toPandas().to_csv('Output/Hv_ratio.csv')
# PeakHigh pRice with sql

dt1.createOrReplaceTempView("Stock")
#on transforme la DataTable en table
spark.sql("""SELECT Date FROM Stock ORDER BY High Desc""").show(1)
#solution

#7) Calcul de la moyenne
dt1.select("Close").summary("mean").show()
# Méthode Sql
spark.sql("""SELECT AVG(Close) as AVG_Close  FROM Stock """).show()

#8) Min et Max de Close
dt1.select("Volume").summary("min","max").show()
# Méthode Sql
spark.sql("""SELECT Min(Volume) as Min_Close, Max(Volume) as Max_Close  FROM Stock """)\
    .show()

#09)How many was the Close lower than 60 dollars?
dt1.filter(dt.Close<=60).count()

#methode SQL
spark.sql("""SELECT Count (*)  as Compte FROM Stock WHERE( Close <= 60)""")\
    .show()

#10) % high
n=dt1.count()
print((dt1.filter(dt.High>=80).count()/n)*100)

spark.sql("""SELECT Count(*)/ (SELECT Count(*) FROM Stock)*100 as Pr FROM Stock Where(High>=80) """).show()

#11) Max Per Year
dt2=dt1.groupBy(F.year("Date").alias("Année")).agg(F.max("High").alias("Max_High")).sort(F.year("Date"))
dt2.toPandas().to_csv('Output/Max_Ans.csv')

#methode SQL
spark.sql("""SELECT  Year(Date) as Annee, max(High) as Max_High FROM Stock \
          Group By Annee Order by Annee  """).show()

#12) average Close for each Calendar Month

dt6=dt.groupBy(F.last_day('date').alias("Moi_Annee"))\
.agg({'Close': 'mean'})\
.sort(F.last_day('date'))\

dt3=dt6.select(F.date_format("Moi_Annee","yyyy-MM").alias("Annee"),col("avg(Close)").alias("AvG_Close"))\
   .show()
dt3=dt1.toPandas().to_csv('Output/Moy_Mois.csv')

#methode SQL#methode SQL
spark.sql("""SELECT SUBSTR(Date,1,7) as Annee, avg(Close) as Avg_Close 
FROM Stock \
Group By Annee Order by Annee""").show()

##Arreter Spark
spark.stop()