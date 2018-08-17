package pm.spark.etl.posh

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class EtlReader(sparkPathToFrame: (SparkSession, String, Option[StructType]) => DataFrame, spark: SparkSession) {
  var schema: Option[StructType] = None

  def withSchema(schema: StructType): EtlReader = {
    this.schema = Option(schema)
    this
  }

  def extract(path: String) = sparkPathToFrame(spark, path, schema)
}

object SimpleAvroReader {
  private def sparkPathToFrame(spark: SparkSession, path: String, schema: Option[StructType] = None) = {
    BasicDataFrameReader(spark, schema).format("com.databricks.spark.avro").load(path)
  }

  def apply(spark: SparkSession): EtlReader = new EtlReader(sparkPathToFrame, spark)
}

object SimpleJsonReader {
  private def sparkPathToFrame(spark: SparkSession, path: String, schema: Option[StructType] = None) = {
    BasicDataFrameReader(spark, schema).json(path)
  }

  def apply(spark: SparkSession): EtlReader = new EtlReader(sparkPathToFrame, spark)
}

object BasicDataFrameReader {
  def apply(spark: SparkSession, schema: Option[StructType] = None): DataFrameReader = {
    val dataFrameReader = spark.read
    schema.map(dataFrameReader.schema).getOrElse(dataFrameReader)
  }
}