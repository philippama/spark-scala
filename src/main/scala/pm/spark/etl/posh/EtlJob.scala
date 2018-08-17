package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, DataFrameReader}

class EtlJob(reader: SparkReader, writer: SparkWriter) {

  private var transformer: DataFrame => DataFrame = identity()

  def withTransformer(transformer: DataFrame => DataFrame): EtlJob = {
    this.transformer = transformer
    this
  }

  def identity()(df: DataFrame): DataFrame = df

  def run(sourcePath: String, destPath: String): Unit = {
    val df = reader.extract(sourcePath)
      .transform(transformer)
    writer.load(df, destPath)
  }
}

object EtlJob {
  def apply(reader: SparkReader, writer: SparkWriter) = new EtlJob(reader, writer)
}
