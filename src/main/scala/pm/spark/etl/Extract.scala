package pm.spark.etl

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class Extract(sparkSession: SparkSession) {

  def fromJson(sourcePath: String): DataFrame = {
    sparkSession.read.json(sourcePath)
  }

  def fromJson(sourcePath: String, schema: StructType): DataFrame = {
    sparkSession.read.schema(schema).json(sourcePath)
  }

  def fromAvro(sourcePath: String): DataFrame = {
    sparkSession.read.format("com.databricks.spark.avro").load(sourcePath)
  }

}
