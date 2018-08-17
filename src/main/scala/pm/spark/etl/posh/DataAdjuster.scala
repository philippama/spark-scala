package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataAdjuster(reader: SparkReader, writer: SparkWriter, tempDataWriter: SparkWriter, tempDataReader: SparkReader) {

  private var transformer: DataFrame => DataFrame = identity()

  def withTransformer(transformer: DataFrame => DataFrame): DataAdjuster = {
    this.transformer = transformer
    this
  }

  def identity()(df: DataFrame): DataFrame = df

  def run(sourcePath: String, tempPath: String): Unit = {
    EtlJob(reader, tempDataWriter).run(sourcePath, tempPath)
    EtlJob(tempDataReader, writer).run(tempPath, sourcePath)
  }
}

object DataAdjuster {
  def apply(spark: SparkSession, reader: SparkReader, writer: SparkWriter): DataAdjuster = {
    val tempDataWriter = SimpleAvroWriter()
    val tempDataReader = SimpleAvroReader(spark)
    new DataAdjuster(reader, writer, tempDataWriter, tempDataReader)
  }
}