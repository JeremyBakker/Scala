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