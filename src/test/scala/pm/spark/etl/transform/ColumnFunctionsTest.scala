package pm.spark.etl.transform

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.scalatest.FunSpec

class ColumnFunctionsTest extends FunSpec {
  describe("anonymising coordinates") {
    it("rounds HALF_UP to 5 decimal places") {

      // Given
      val spark = localSparkSession

      val testRows = Array(
        Row(51.4122929339),
        Row(-0.3075169003)
      )

      val testSchema = StructType(Seq(
        StructField("coordinate", DoubleType, nullable = true)
      ))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), testSchema)

      // When
      val actualDf = df
        .withColumn("truncatedCoordinate", ColumnFunctions.anonymiseCoordinate()(col("coordinate")))

      // Then
      val expectedRows = Array(
        Row(51.41229),
        Row(-0.30752)
      )

      val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), testSchema)

      assertResult(expectedDf.collect)(actualDf.select("truncatedCoordinate").collect)
    }

    it("handles null coordinates") {

      // Given
      val spark = localSparkSession

      val testRows = Array(
        Row(51.41229),
        Row(null)
      )

      val testSchema = StructType(Seq(
        StructField("coordinate", DoubleType, nullable = true)
      ))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), testSchema)

      // When
      val actualDf = df
        .withColumn("truncatedCoordinate", ColumnFunctions.anonymiseCoordinate()(col("coordinate")))

      // Then
      val expectedRows = Array(
        Row(51.41229),
        Row(null)
      )

      val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), testSchema)

      assertResult(expectedDf.collect)(actualDf.select("truncatedCoordinate").collect)
    }
  }

  //TODO: refactor out a trait for this - or use SharedSparkContext from spark-testing-base?
  private def localSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.port", 7077)
      .getOrCreate()
  }

}
