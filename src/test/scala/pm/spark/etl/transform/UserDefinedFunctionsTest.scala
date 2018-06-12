package pm.spark.etl.transform

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.FunSpec

class UserDefinedFunctionsTest extends FunSpec {

  describe("anonymising postcodes") {
    it("truncates the last character of postcodes") {

      // Given
      val spark = localSparkSession

      val testRows = Array(
        Row("postcode1x"),
        Row("postcode2x"),
        Row("postcode3x")
      )

      val testSchema = StructType(Seq(
        StructField("postcode", StringType, nullable = true)
      ))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), testSchema)

      // When
      val actualDf = df
        .withColumn("truncatedPostcode", UserDefinedFunctions.anonymisePostcode(col("postcode")))

      // Then
      val expectedRows = Array(
        Row("postcode1"),
        Row("postcode2"),
        Row("postcode3")
      )

      val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), testSchema)

      assertResult(expectedDf.collect)(actualDf.select("truncatedPostcode").collect)
    }

    it("handles null and empty postcodes") {

      // Given
      val spark = localSparkSession

      val testRows = Array(
        Row("postcodex"),
        Row(""),
        Row(null)
      )

      val testSchema = StructType(Seq(
        StructField("postcode", StringType, nullable = true)
      ))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), testSchema)

      // When
      val actualDf = df
        .withColumn("truncatedPostcode", UserDefinedFunctions.anonymisePostcode(col("postcode")))

      // Then
      val expectedRows = Array(
        Row("postcode"),
        Row(""),
        Row(null)
      )

      val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), testSchema)

      assertResult(expectedDf.collect)(actualDf.select("truncatedPostcode").collect)
    }

    it("truncates the last character of postcodes in select") {

      // Given
      val spark = localSparkSession

      import spark.implicits._

      val testRows = Array(
        Row("postcode1x"),
        Row("postcode2x"),
        Row("postcode3x")
      )

      val testSchema = StructType(Seq(
        StructField("postcode", StringType, nullable = true)
      ))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), testSchema)

      // When
      val actualDf = df
        .select(
          $"postcode",
          UserDefinedFunctions.anonymisePostcode(col("postcode")).as("truncatedPostcode")
        )

      // Then
      val expectedRows = Array(
        Row("postcode1"),
        Row("postcode2"),
        Row("postcode3")
      )

      val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), testSchema)

      assertResult(expectedDf.collect)(actualDf.select("truncatedPostcode").collect)
    }
  }

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
        .withColumn("truncatedCoordinate", UserDefinedFunctions.anonymiseCoordinate(col("coordinate")))

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
        .withColumn("truncatedCoordinate", UserDefinedFunctions.anonymiseCoordinate(col("coordinate")))

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
