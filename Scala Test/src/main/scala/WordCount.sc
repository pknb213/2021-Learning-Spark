trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark app")
      .getorCreate()
  }
}

object WordCount extends SparkSessionWrapper{

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val textFile = spark.read.textFile("src/main/resources/words.txt")
    val wordCounts = textFile.flatMap(line => line.split(" "))
      .groupByKey(identity)
      .count()

    wordCounts.show()
  }
}