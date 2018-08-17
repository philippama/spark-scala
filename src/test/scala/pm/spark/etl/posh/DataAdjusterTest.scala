package pm.spark.etl.posh

import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec
import pm.spark.utils.LocalSparkSession

class DataAdjusterTest extends FunSpec with LocalSparkSession {

  it("alters an Avro data set in situ") {

    // Given
    val spark = localSparkSession
    val testDir = Files.createTempDirectory("testdata")
    val sourceDir = Files.createDirectory(Paths.get(testDir.toString, "source"))
    val tempDir = Files.createDirectory(Paths.get(testDir.toString, "temp"))

    val testDf = createTestDataFrame(spark)
    testDf.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(sourceDir.toString)

    val schema = StructType(Seq(
      StructField("description", StringType, nullable = true),
      StructField("numThings", IntegerType, nullable = true)
    ))

    val dataAdjuster = DataAdjuster(spark, SimpleAvroReader(spark).withSchema(schema), SimpleAvroWriter())

    // When
    dataAdjuster.run(sourceDir.toString, tempDir.toString)

    // Then
    val df = localSparkSession.read.format("com.databricks.spark.avro").load(tempDir.toString)
    val actualRows = df.sort("description").collect

    val expectedRows = Array(
      Row("thing-1", 1),
      Row("thing-2", 2),
      Row("thing-3", 3),
      Row("thing-4", 4),
      Row("thing-5", 5)
    )
    assertResult(expectedRows)(actualRows)

    FileUtils.deleteDirectory(testDir.toFile)

    spark.stop()
  }

  it("changed the format of a data set in situ") {

    // Given
    val spark = localSparkSession
    val testDir = Files.createTempDirectory("testdata")
    val sourceDir = Files.createDirectory(Paths.get(testDir.toString, "source"))
    val tempDir = Files.createDirectory(Paths.get(testDir.toString, "temp"))

    val testDf = createTestDataFrame(spark)
    testDf.write
      .mode(SaveMode.Overwrite)
      .json(sourceDir.toString)

    val dataAdjuster = DataAdjuster(spark, SimpleJsonReader(spark), SimpleParquetWriter())

    // When
    dataAdjuster.run(sourceDir.toString, tempDir.toString)

    // Then
    val df = localSparkSession.read.parquet(sourceDir.toString)
    val actualRows = df.select("description", "numThings", "comment").sort("description").collect

    val expectedRows = Array(
      Row("thing-1", 1, "comment-1"),
      Row("thing-2", 2, "comment-2"),
      Row("thing-3", 3, "comment-3"),
      Row("thing-4", 4, "comment-4"),
      Row("thing-5", 5, "comment-5")
    )
    assertResult(expectedRows)(actualRows)

    FileUtils.deleteDirectory(testDir.toFile)

    spark.stop()
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