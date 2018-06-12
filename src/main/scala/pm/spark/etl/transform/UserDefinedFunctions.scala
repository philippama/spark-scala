package pm.spark.etl.transform

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UserDefinedFunctions {
  // NB UDFs are not performant: https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-udfs-blackbox.html
  private def truncatePostcode(postcode: String): Option[String] = {
    Option(postcode).map(_.dropRight(1))
  }

  def anonymisePostcode: UserDefinedFunction = udf[Option[String], String](truncatePostcode)
}
