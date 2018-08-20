package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataAdjuster(reader: SparkReader, writer: SparkWriter, tempDataWriter: SparkWriter, tempDataReader: SparkReader) {

  private var postReadTransformer: DataFrameTransformer = IdentityTransformer()
  private var preWriteTransformer: DataFrameTransformer = IdentityTransformer()

  def withPostReadTransformer(transformer: DataFrameTransformer): DataAdjuster = {
    this.postReadTransformer = transformer
    this
  }

  def withPreWriteTransformer(transformer: DataFrameTransformer): DataAdjuster = {
    this.preWriteTransformer = transformer
    this
  }

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
