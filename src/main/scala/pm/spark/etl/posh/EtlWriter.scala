package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SaveMode}

class EtlWriter(dataFrameToFile: (DataFrame, String) => Unit) {
  def load(dataFrame: DataFrame, path: String): Unit = dataFrameToFile(dataFrame, path)
}

object DefaultAvroWriter {
  private def dataFrameToFile(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(path)
  }
  def apply(): EtlWriter = new EtlWriter(dataFrameToFile)
}

object DefaultParquetWriter {
  private def dataFrameToFile(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }
  def apply(): EtlWriter = new EtlWriter(dataFrameToFile)
}