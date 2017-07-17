val rawblocks = sqlContext.read.format("csv").option("header", "true")
  .option("inferSchema", "true").load("/FileStore/tables/2xkgsqsy1500308516009")
rawblocks.first

val head = rawblocks.take(10)

head.length

head.foreach(println)

val line = head(5)

val new_line = line.mkString(" ")

val pieces = new_line.split(' ')

pieces.foreach(println)

def toDouble(s: String) = {if ("?".equals(s)) Double.NaN else s.toDouble}

def parse(new_line: String) = {
  val pieces = new_line.split(' ');
  val id1 = pieces(0).toInt;
  val id2 = pieces(1).toInt;
  val scores = pieces.slice(2,11).map(toDouble);
  val matched = pieces(11).toBoolean;
  (id1, id2, scores, matched)
}

val tup = parse(new_line)

tup._1

tup.productElement(0)



val nums = List(1,2,3,4,5).filter(_ < 4).map(_ * 2)

for (c <- "hello") println(c)

"hello".getBytes.foreach(println)

val result = "hello world".filter(_ != 'l')

"scala".drop(2).take(2).capitalize

