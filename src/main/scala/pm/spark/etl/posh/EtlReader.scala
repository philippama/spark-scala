package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SparkSession}

class EtlReader(sparkPathToFrame: (SparkSession, String) => DataFrame, spark: SparkSession) {
  def extract(path: String) = sparkPathToFrame(spark, path)
}

object SimpleAvroReader {
  private def sparkPathToFrame(spark: SparkSession, path: String) = {
    spark.read.format("com.databricks.spark.avro").load(path)
  }
  def apply(spark: SparkSession): EtlReader = new EtlReader(sparkPathToFrame, spark)
}

object SimpleJsonReader {
  private def sparkPathToFrame(spark: SparkSession, path: String) = {
    spark.read.json(path)
  }
  def apply(spark: SparkSession): EtlReader = new EtlReader(sparkPathToFrame, spark)
}