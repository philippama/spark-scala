package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SaveMode}

class SparkWriter(dataFrameToFile: (DataFrame, String) => Unit) {
  def load(dataFrame: DataFrame, path: String): Unit = dataFrameToFile(dataFrame, path)
}

object SimpleAvroWriter {
  private def dataFrameToFile(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(path)
  }
  def apply(): SparkWriter = new SparkWriter(dataFrameToFile)
}

object SimpleParquetWriter {
  private def dataFrameToFile(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }
  def apply(): SparkWriter = new SparkWriter(dataFrameToFile)
}