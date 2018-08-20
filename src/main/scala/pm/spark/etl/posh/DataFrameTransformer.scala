package pm.spark.etl.posh

import org.apache.spark.sql.DataFrame

trait DataFrameTransformer {
  def transform(df: DataFrame): DataFrame
}

class IdentityTransformer extends DataFrameTransformer {
  def transform(df: DataFrame): DataFrame = df
}

object IdentityTransformer {
  def apply(): DataFrameTransformer = new IdentityTransformer()
}