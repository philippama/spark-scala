package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SparkSession}

class EtlReader(sparkPathToFrame: (SparkSession, String) => DataFrame, spark: SparkSession, dataPath: String) {
  def extract = sparkPathToFrame(spark, dataPath)
}

object DefaultAvroReader {
  private def sparkPathToFrame(spark: SparkSession, dataPath: String) = {
    spark.read.format("com.databricks.spark.avro").load(dataPath)
  }
  def apply(spark: SparkSession, dataPath: String): EtlReader = new EtlReader(sparkPathToFrame, spark, dataPath)
}

object DefaultJsonReader {
  private def sparkPathToFrame(spark: SparkSession, path: String) = {
    spark.read.json(path)
  }
  def apply(spark: SparkSession, dataPath: String): EtlReader = new EtlReader(sparkPathToFrame, spark, dataPath)
}