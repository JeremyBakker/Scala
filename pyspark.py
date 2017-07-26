# Create RDD
lines = sc.textFile("README.md")

# Basic Filter with lambda
pythonLines = lines.filter(lambda line: "Python in line")

# Basic Count
pythonLines.count()

# Pull the first line
pythonLines.first()

# Persist an RDD in memory -- useful when needing to use an intermediate RDD
# multiple times
pythonLines.persist

# Parallelize data to create an RDD
lines = sc.parallelize(["pandas", "i like pandas"])

# Create an RDD of a log file and filter on "ERROR" and "WARNING"
inputRDD = sc.textFile("log.txt")
errorsRDD = inputRDD.filter(lambda x: "ERROR" in x)
warningsRDD = inputRDD.filter(lambda x: "WARNING" in x)
badLinesRDD = errorsRDD.union(warningsRDD)
# Print results
print("Input had " + str(badLinesRDD.count()) + " concerning lines")
print("Here are 10 examples:")
for line in badLinesRDDtake(10):
    print(line)

# Filter by passing in a function
def containsError(line):
    return "ERROR" in line
word = inputRDD.filter(containsError)

# Basic map function
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect() 
for num in squared:
    print ("%i " % (num))

# Basic flat map function
lines = sc.parallelize(["hello world", "hi"]) 
words = lines.flatMap(lambda line: line.split(" ")) 
words.first()

nums_two = sc.parallelize([3,4,5,6])

nums.union(nums_two)
nums.intersection(nums_two)
nums.cartesian(nums_two).collect()

nums.reduce(lambda x,y: x + y)


# Read in data from csv
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/Users/jeremybakker/Desktop/flight_data.csv")

# Transform dataframe into a table
df.createOrReplaceTempView("flight_data_2017")

# SQL Query to find number of originating flights by destination
sql = spark.sql("""SELECT ORIGIN, count(1) FROM flight_data_2017 GROUP BY ORIGIN ORDER BY count(1) DESC""")
# Same query with dataframe
from pyspark.sql.functions import desc
dfQuery = df.groupBy("ORIGIN").count().sort(desc("count"))

# Show the Spark physical plans - The underlying plans are the same.
sql.explain()
# OUT
# == Physical Plan ==
# *Sort [count(1)#96L DESC NULLS LAST], true, 0
# +- Exchange rangepartitioning(count(1)#96L DESC NULLS LAST, 200)
#    +- *HashAggregate(keys=[ORIGIN#13], functions=[count(1)])
#       +- Exchange hashpartitioning(ORIGIN#13, 200)
#          +- *HashAggregate(keys=[ORIGIN#13], functions=[partial_count(1)])
#             +- *FileScan csv [ORIGIN#13] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/jeremybakker/Desktop/flight_data.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ORIGIN:string>
dfQuery.explain()
# OUT== Physical Plan ==
# *Sort [count#292L DESC NULLS LAST], true, 0
# +- Exchange rangepartitioning(count#292L DESC NULLS LAST, 200)
#    +- *HashAggregate(keys=[ORIGIN#13], functions=[count(1)])
#       +- Exchange hashpartitioning(ORIGIN#13, 200)
#          +- *HashAggregate(keys=[ORIGIN#13], functions=[partial_count(1)])
#             +- *FileScan csv [ORIGIN#13] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/jeremybakker/Desktop/flight_data.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ORIGIN:string>

# Print schema of the dataframe, which deines the column names and types of a DataFrame
df.printSchema()
df.schema

# Two ways to create columns
from pyspark.sql.functions import col, column
col("columnName")
column("columnName")

# See all columns listed
df.columns

# See first row
df.first()

# Create a row 
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

# Access data from Row
myRow[0]

# Add a column that specifies whether the destination and origin country are the same
df.selectExpr("*", "(DEST_COUNTRY = ORIGIN_COUNTRY) as WithinCountry").show(2)

# Aggregate over the entire dataframe with selectExpr
df.selectExpr("avg(NONSTOP_MILES)", "count(distinct(DEST_COUNTRY))").show()

# Add a literal value with an alias
df.select(expr("*"), lit(1).alias("One")).show(2)

# Add a column using withColumn - withColumn takes to arguments: name and expression to create the value for a given row
df.withColumn("numberOne", lit(1)).show(2)

# Rename column
df.withColumnRenamed("DEST_COUNTRY", "Destination Country")

# Drop column
df.drop("DEST_COUNTRY")

# Two different ways to filter - explain() outputs are the same
colCondition = df.filter(col("NONSTOP_MILES") > 1000)
conditional = df.where(col("NONSTOP_MILES") > 1000)

# Find number of distinct origins
df.select("ORIGIN").distinct().count()
spark.sql("""SELECT count(distinct origin) from flight_data_2017""").show()

#Random Sample
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed)

# Randomly Split Data
dataFrames = df.randomSplit([0.25, 0.75],seed)

# Sort
df.sort("NONSTOP_MILES")

# sortWithinPartitions can help with optimization
df.sortWithinPartitions("NONSTOP_MILES")

# limit
df.limit(5).show()

# find number of partitions
df.rdd.getNumPartitions()

# repartition
df.repartition(5)

# repartition by column
df.repartition(col("NONSTOP_MILES"))

# repartition by column with defined number of partitions
df.repartition(5, col("NONSTOP_MILES"))

# combine partitions without a full shuffle
df.repartition(5, col("NONSTOP_MILES")).coalesce(2)

# load retail data
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Users/jeremybakker/Desktop/WA_Sales_Products.csv")

# use Booleans to filter data - simple
df.where(col("Product line") == "Camping Equipment").select("Order method type", "Retailer type", "Product").show(5, False)

# filter with and/or
eyeWearFilter = col("Product type") == "Eyewear"
revenueFilter = col("Revenue") > 4000
quarterFilter = col("Quarter") == "Q1 2012"
df.withColumn("isProfitable", revenueFilter & (eyeWearFilter | quarterFilter)).where("isProfitable").select("Product", "isProfitable")

# working with numbers
df.select(expr("Product type"),fabricatedQuantity.alias("realQuantity"), col("Quantity"))

# round, lit, bround
from pyspark.sql.functions import lit, round, bround
df.select(round(col("Revenue"), 1).alias("rounded"), col("Revenue")).show(5)
df.select(round(lit("2.5")),bround(lit("2.5"))).show(2)

# Pearson coefficient for quantityt and revenue
df.select(corr("Quantity", "Revenue")).show()

# summary statistics
df.describe().show()

# capitalize words
from pyspark.sql.functions import initcap
df.select(initcap(col("Product line"))).show(2, false)

# upper,lower
from pyspark.sql.functions import upper,lower
df.select(col("Product type"), lower(col("Product type")), upper(lower(col("Product type")))).show(2)

# add or remove white space
from pyspark.sql.functions import lit,trim,ltrim,rtrim,rpad,lpad
df.select(ltrim(lit("     HELLO     ")).alias("ltrim"),
    rtrim(lit("     HELLO     ")).alias("rtrim"),
    trim(lit("     HELLO     ")).alias("trim"),
    lpad(lit("HELLO"), 3," ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# regex replace
from pyspark.sql.functions import regexp_replace
df.select(regexp_replace(col("Retailer type"), "Outdoors", "Indoors").alias("Moved Indoors"),col("Retailer type")).show(2)

# translate characters
from pyspark.sql.functions import translate
df.select(translate(col("Product type"), "okin", "1347"),col("Product type")).show(2)

# find items with "Master" in the product column
from pyspark.sql.functions import instr
containsMaster = instr(col("Product"), "Master") >= 1
df.withColumn("hasMaster", containsMaster).filter("hasMaster").select("Product").show(3, False)

# simple UDF - better to write in Scala to control memory costs
val udfExampleDf = spark.range(5).toDF("num")

def power3(number:Double):Double = {
    number * number * number
}

val power3udf = udf(power3(_:Double):Double)
udfExampleDF.select(power3udf(col("num"))).show()

# Aggregations
from pyspark.sql.functions import count
df.select(count("Product")).collect()

from pyspark.sql.functions import countDistinct
df.select(countDistinct("Product")).collect()

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("Product", 0.1)).collect()

from pyspark.sql.functions import first,last
df.select(first("Product"), last("Product")).collect()

from pyspark.sql.functions import min, max
df.select(min("Gross margin"), max("Gross margin")).collect()

from pyspark.sql.functions import sum
df.select(sum("Revenue")).show()

from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show()

from pyspark.sql.functions import sum, count, avg, expr
df.select(count("Quantity").alias("total_transactions"),sum("Quantity").alias("total_purchases"),avg("Quantity").alias("avg_purchases"),expr("mean(Quantity)").alias("mean_purchases")).selectExpr("total_purchases/total_transactions","avg_purchases","mean_purchases").collect()

from pyspark.sql.functions import var_pop, stddev_pop, var_samp, stddev_samp
df.select(var_pop("Quantity"),var_samp("Quantity"),stddev_pop("Quantity"),stddev_samp("Quantity")).collect()

df.groupBy("Product","Product type").count().show()

df.groupBy("Product").agg(count("Quantity").alias("quan"),expr("count(Quantity)")).show()

df.groupBy("Product").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)")).show()


# Joins
person = spark.createDataFrame([(0, "Bill Chambers", 0, [100]), (1, "Matei Zaharia", 1, [500, 250, 100]), (2, "Michael Armbrust", 1, [250, 100])]).toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([(0,"Masters", "School of Information", "UC Berkeley"),(2, "Masters", "EECS", "UC Berkeley"),(1, "Ph.D.", "EECS", "UC Berkeley")]).toDF("id", "degree","department","school")
sparkStatus = spark.createDataFrame([(500, "Vice President"),(250, "PMC Member"),(100,"Contributor")]).toDF("id","status")
person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# Inner Joins
joinExpression = person.graduate_program == graduateProgram.id
person.join(graduateProgram, joinExpression).show()
person.join(graduateProgram, joinExpression, "inner").show()
person.join(graduateProgram, joinExpression, "outer").show()
person.join(graduateProgram, joinExpression, "left_outer").show()
