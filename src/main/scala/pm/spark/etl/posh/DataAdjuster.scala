package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataAdjuster(reader: SparkReader, writer: SparkWriter, tempDataWriter: SparkWriter, tempDataReader: SparkReader) {

  private var postReadTransformer: DataFrame => DataFrame = identity()
  private var preWriteTransformer: DataFrame => DataFrame = identity()

  def withPostReadTransformer(transformer: DataFrame => DataFrame): DataAdjuster = {
    this.postReadTransformer = transformer
    this
  }

  def withPreWriteTransformer(transformer: DataFrame => DataFrame): DataAdjuster = {
    this.preWriteTransformer = transformer
    this
  }

  def identity()(df: DataFrame): DataFrame = df

  def run(sourcePath: String, tempPath: String): Unit = {
    EtlJob(reader, tempDataWriter).withTransformer(postReadTransformer).run(sourcePath, tempPath)
    EtlJob(tempDataReader, writer).withTransformer(preWriteTransformer).run(tempPath, sourcePath)
  }
}

object DataAdjuster {
  def apply(spark: SparkSession, reader: SparkReader, writer: SparkWriter): DataAdjuster = {
    val tempDataWriter = SimpleAvroWriter()
    val tempDataReader = SimpleAvroReader(spark)
    new DataAdjuster(reader, writer, tempDataWriter, tempDataReader)
  }
}