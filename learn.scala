// Read the csv
val rawblocks = sqlContext.read.format("csv").option("header", "true")
  .option("inferSchema", "true").load("/FileStore/tables/2xkgsqsy1500308516009")
rawblocks.first

// Pull the first 10 records
val head = rawblocks.take(10)

// Verify that we've pulled the correct data
head.length
head.foreach(println)

// Pull one row
val line = head(5)

// Convert the row to a string
val new_line = line.mkString(" ")

// Pull individual elements from the row
val pieces = new_line.split(' ')

// Print to confirm that we've pulled the correct data
pieces.foreach(println)

// Define a function to convert "?" to a Double field with "NaN" as a value
def toDouble(s: String) = {if ("?".equals(s)) Double.NaN else s.toDouble}

// Create a case class for the record types to make the code more explicit
case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

// Define a function to parse each element of the data, assigning the correct field values
def parse(new_line: String) = {
  val pieces = new_line.split(' ');
  val id1 = pieces(0).toInt;
  val id2 = pieces(1).toInt;
  val scores = pieces.slice(2,11).map(toDouble);
  val matched = pieces(11).toBoolean;
  MatchData(id1, id2, scores, matched)
}

// Parse the data and assign it to a variable
val tup = parse(new_line)

// Pull individual values from the tuple
tup._1
tup.productElement(0)

// Check the length of the tuple
tup.productArity

//
val mds = head.map(x => x.mkString(" "))

//
val mds_parsed = mds.map(x => parse(x))

val string_for_parsed = rawblocks.map(line => line.mkString(" "))

val parsed = string_for_parsed.map(line => parse(line))

val rdd = parsed.rdd

val matchCounts = rdd.map(md => md.matched).countByValue()

val matchCountsSeq = matchCounts.toSeq

matchCountsSeq.sortBy(_._1).foreach(println)

rdd.map(md => md.scores(0)).stats()

import java.lang.Double.isNaN
rdd.map(md => md.scores(0)).filter(!isNaN(_)).stats()









val nums = List(1,2,3,4,5).filter(_ < 4).map(_ * 2)

for (c <- "hello") println(c)

"hello".getBytes.foreach(println)

val result = "hello world".filter(_ != 'l')

"scala".drop(2).take(2).capitalize

