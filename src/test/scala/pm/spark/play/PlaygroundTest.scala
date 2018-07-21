package pm.spark.play

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

case class TestClass(description: String, numThings: Int)

class PlayTest extends FunSpec {

  describe("does something") {
    it("playground for experimenting") {

      // Given
      val spark = localSparkSession

      val testDf: DataFrame = createTestDataFrame(spark)

      // When
      val schema: StructType = testDf.schema


      // Then

      spark.stop()
    }

    it("empty DataFrame") {

      // Given
      val spark = localSparkSession

      val df: DataFrame = spark.emptyDataFrame

      // When
      val isEmpty = df.rdd.isEmpty


      // Then
      assertResult(true)(isEmpty)

      spark.stop()
    }

    it("extracts value from row with filter using Spark SQL") {
      // Given
      val spark = localSparkSession

      val testDf: DataFrame = createTestDataFrame(spark)

      // When
      import spark.implicits._
      val result = testDf.filter("description = 'thing-2'")
          .first()
          .getString(0)


      // Then
      assertResult("thing-2")(result)

      spark.stop()
    }

    it("extracts value from row with filter using implicits") {
      // Given
      val spark = localSparkSession

      val testDf: DataFrame = createTestDataFrame(spark)

      // When
      import spark.implicits._
      val result = testDf.filter($"description" === "thing-2")
          .first()
          .getString(0)


      // Then
      assertResult("thing-2")(result)

      spark.stop()
    }

    it("selects array of columns") {
      // Given
      val spark = localSparkSession

      val testDf: DataFrame = createTestDataFrame(spark)
      val columns = Array("description", "comment")

      // When
      val result = testDf.select(columns.head, columns.tail: _*)
      testDf.groupBy("description")


      // Then
      val actualRows: Array[Row] = result.sort("description").collect

      val expectedRows = Array(
        Row("thing-1", "comment-1"),
        Row("thing-2", "comment-2"),
        Row("thing-3", "comment-3"),
        Row("thing-4", "comment-4"),
        Row("thing-5", "comment-5")
      )
      assertResult(expectedRows)(actualRows)

      spark.stop()
    }

    it("finds the maximum of a column") {
      // Given
      val spark = localSparkSession

      val testDf: DataFrame = createTestDataFrame(spark)
      val columns = Array("description", "comment")

      // When
      val result = testDf.select(columns.head, columns.tail: _*)

      // Then
      val actualRows: Array[Row] = result.sort("description").collect

      val expectedRows = Array(
        Row("thing-1", "comment-1"),
        Row("thing-2", "comment-2"),
        Row("thing-3", "comment-3"),
        Row("thing-4", "comment-4"),
        Row("thing-5", "comment-5")
      )
      assertResult(expectedRows)(actualRows)

      spark.stop()
    }
  }

//TODO : refactor out a trait for this - or use SharedSparkContext from spark-testing-base?
  private def localSparkSession = {
    SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.port", 7077)
      .getOrCreate()
  }

  private def createTestDataFrame(spark: SparkSession) = {
    val testSchema = StructType(Seq(
      StructField("description", StringType, nullable = true),
      StructField("numThings", IntegerType, nullable = true),
      StructField("comment", StringType, nullable = true)
    ))

    val testRows = Array(
      Row("thing-1", 1, "comment-1"),
      Row("thing-2", 2, "comment-2"),
      Row("thing-3", 3, "comment-3"),
      Row("thing-4", 4, "comment-4"),
      Row("thing-5", 5, "comment-5")
    )

    spark.createDataFrame(spark.sparkContext.parallelize(testRows), testSchema)
  }

}

