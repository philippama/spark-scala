package pm.spark.etl.transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnFunctions {

  def anonymiseCoordinate()(col: Column): Column = {
    round(col, 5)
  }
}
