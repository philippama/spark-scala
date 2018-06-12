package pm.spark.etl.transform

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UserDefinedFunctions {
  // NB UDFs are not performant: https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-udfs-blackbox.html
  private def truncatePostcode(postcode: String) = {
    Option(postcode).map(_.dropRight(1))
  }

  def anonymisePostcode: UserDefinedFunction = udf[Option[String], String](truncatePostcode)

  private def truncateCoordinate(coordinate: Double): Option[Double] = {
    truncateCoordinate(coordinate, 5)
  }

  private def truncateCoordinate(coordinate: Double, decimalPlaces: Double): Option[Double] = {
    Option(coordinate)
      .map(coord => {
      val factor = Math.pow(10, decimalPlaces)
      Math.round(coord * factor).toDouble / factor
    } )
  }

  def anonymiseCoordinate: UserDefinedFunction = udf[Option[Double], Double](truncateCoordinate)
}
