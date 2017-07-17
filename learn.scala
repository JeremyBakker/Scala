val rawblocks = sqlContext.read.format("csv").load("/FileStore/tables/2xkgsqsy1500308516009")

rawblocks.first

val head = rawblocks.take(10)

head.length

