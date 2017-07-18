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

inputRDD = sc.textFile("log.txt")
errorsRDD = inputRDD.filter(lambda x: "ERROR" in x)
warningsRDD = inputRDD.filter(lambda x: "WARNING" in x)
badLinesRDD = errorsRDD.union(warningsRDD)
