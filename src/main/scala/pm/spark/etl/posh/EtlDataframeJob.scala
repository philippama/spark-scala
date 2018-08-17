package pm.spark.etl.posh

import org.apache.spark.sql.{DataFrame, DataFrameReader}

class EtlDataframeJob(df: DataFrame, writer: DataFrame => Nothing) {

  private var transformer: DataFrame => DataFrame = _

  def withTransformer(transformer: DataFrame => DataFrame = identity()): Unit = {
    this.transformer = transformer
  }

  def identity()(df: DataFrame): DataFrame = df

  def transform(): Unit = {

  }

}
