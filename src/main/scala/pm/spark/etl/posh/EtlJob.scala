package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, DataFrameReader}

class EtlJob(reader: EtlReader, writer: EtlWriter) {

  private var transformer: DataFrame => DataFrame = identity()

  def withTransformer(transformer: DataFrame => DataFrame): EtlJob = {
    this.transformer = transformer
    this
  }

  def identity()(df: DataFrame): DataFrame = df

  def transform(sourcePath: String, destPath: String): Unit = {
    val df = reader.extract(sourcePath)
      .transform(transformer)
    writer.load(df, destPath)
  }

  def transformToSameLocation(reader: DataFrameReader, writer: DataFrame => Nothing, transformer: DataFrame => DataFrame = identity()): Unit = {
    // Can this work with a format change?
  }

}
