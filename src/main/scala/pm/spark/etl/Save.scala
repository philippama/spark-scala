package pm.spark.etl

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class Save(sparkSession: SparkSession) {

  def asJson(dataFrame: DataFrame, destPath: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .json(destPath)
  }

  def asAvro(dataFrame: DataFrame, destPath: String): Unit = {
    dataFrame.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(destPath)
  }
}
