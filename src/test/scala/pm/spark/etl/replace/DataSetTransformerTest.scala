package pm.spark.etl.replace

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.scalatest.FunSpec
import pm.spark.utils.LocalSparkSession

class DataSetTransformerTest extends FunSpec with LocalSparkSession {

  describe("transform for DataFrame") {
    it("transforms a DataFrame") {

      // Given
      val spark = localSparkSession

      def withNewColumn()(df: DataFrame): DataFrame = {
        df.withColumn("newColumn", lit("new column"))
      }

      val transformer = DataSetTransformer(null, "", "", withNewColumn())

      val testDf: DataFrame = createTestDataFrame(spark)

      // When
      val df = transformer.transform(testDf)

      // Then
      val actualRows: Array[Row] = df.sort("description").collect

      val expectedRows = Array(
        Row("thing-1", 1, "comment-1", "new column"),
        Row("thing-2", 2, "comment-2", "new column"),
        Row("thing-3", 3, "comment-3", "new column"),
        Row("thing-4", 4, "comment-4", "new column"),
        Row("thing-5", 5, "comment-5", "new column")
      )
      assertResult(expectedRows)(actualRows)

      spark.stop()
    }
  }

  describe("transform for data set in file") {
    it("replaces an Avro data set with a transformed data set") {

      // Given
      val spark = localSparkSession
      val testDir = Files.createTempDirectory("testdata")
      val tempDir = Files.createTempDirectory("tempdata")

      val testDf: DataFrame = createTestDataFrame(spark)
      testDf.write
        .format("com.databricks.spark.avro")
        .mode(SaveMode.Overwrite)
        .save(testDir.toString)

      def withNewColumn()(df: DataFrame): DataFrame = {
        df.withColumn("newColumn", lit("new column"))
      }

      val dataPath = s"${testDir.toString}"
      val tempDataPath = s"${tempDir.toString}"
      val transformer = DataSetTransformer(localSparkSession, dataPath, tempDataPath, withNewColumn())

      // When
      transformer.transform()

      // Then
      val df = localSparkSession.read.format("com.databricks.spark.avro").load(dataPath)
      val actualRows: Array[Row] = df.sort("description").collect

      val expectedRows = Array(
        Row("thing-1", 1, "comment-1", "new column"),
        Row("thing-2", 2, "comment-2", "new column"),
        Row("thing-3", 3, "comment-3", "new column"),
        Row("thing-4", 4, "comment-4", "new column"),
        Row("thing-5", 5, "comment-5", "new column")
      )
      assertResult(expectedRows)(actualRows)

      FileUtils.deleteDirectory(tempDir.toFile)
      FileUtils.deleteDirectory(testDir.toFile)

      spark.stop()
    }
  }

  it("TODO: has not been done yet") {
/*
TODO
- with correct file format
- overwrites existing data
read with schema?
- deletes temporary directory if required?
- reads multiple paths? don't think so
- Split into
    DataSetReader that different file formats
    DataSetWriter that optionally handles re-partitioning etc: DataSetWriter.withPartitionCalculator(...) and different file formats
    DataSetMover
 */
    fail("TODO")
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
