import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  println("I'm Session Wrapper")
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark App")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }
}
