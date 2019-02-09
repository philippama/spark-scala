package pm.spark

import org.apache.spark.sql.SparkSession

trait LocalSpark {
  def localSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.port", 7077)
      .getOrCreate()
  }
  val spark: SparkSession = localSparkSession
}
