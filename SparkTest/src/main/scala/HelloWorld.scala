object HelloWorld extends SparkSessionWrapper {
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val filename = "src/main/resources/words.txt"
    val textFile = spark.read.textFile(filename).as[String]
    val wordCount = textFile.flatMap(line => line.split(" "))
      .groupByKey(identity).count()

    wordCount.show()
//    spark.stop()
  }
}