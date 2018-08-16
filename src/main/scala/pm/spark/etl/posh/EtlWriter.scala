package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SaveMode}

class EtlWriter(dataFrameToFile: (DataFrame, String) => Unit, destPath: String) {
  def load(dataFrame: DataFrame): Unit = dataFrameToFile(dataFrame, destPath)
}

object DefaultAvroWriter {
  private def dataFrameToFile(dataFrame: DataFrame, destPath: String): Unit = {
    dataFrame.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(destPath)
  }
  def apply(destPath: String): EtlWriter = new EtlWriter(dataFrameToFile, destPath)
}

object DefaultParquetWriter {
  private def dataFrameToFile(dataFrame: DataFrame, destPath: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(destPath)
  }
  def apply(destPath: String): EtlWriter = new EtlWriter(dataFrameToFile, destPath)
}