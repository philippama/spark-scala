package pm.spark.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

class Extract(sparkSession: SparkSession) {

  def fromJson(sourcePath: String): DataFrame = {
    sparkSession.read.json(sourcePath)
  }

  def fromAvro(sourcePath: String): DataFrame = {
    sparkSession.read.format("com.databricks.spark.avro").load(sourcePath)
  }

}
