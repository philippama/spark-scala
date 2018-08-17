package pm.spark.etl.posh

import java.nio.file.{Files, Path, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec
import pm.spark.etl.replace.DataSetTransformer
import pm.spark.utils.LocalSparkSession

class EtlJobTest extends FunSpec with LocalSparkSession {

  it("replaces an Avro data set with a transformed data set") {

    // Given
    val spark = localSparkSession
    val testDir = Files.createTempDirectory("testdata")
    val sourceDir = Files.createDirectory(Paths.get(testDir.toString, "source"))
    val destDir = Files.createDirectory(Paths.get(testDir.toString, "dest"))

    val testDf: DataFrame = createTestDataFrame(spark)
    testDf.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(sourceDir.toString)

    def withNewColumn()(df: DataFrame): DataFrame = {
      df.withColumn("newColumn", lit("new column"))
    }

    val etlJob:EtlJob = new EtlJob(SimpleAvroReader(spark), SimpleAvroWriter()).withTransformer(withNewColumn())

    // When
    etlJob.transform(sourceDir.toString, destDir.toString)

    // Then
    val df = localSparkSession.read.format("com.databricks.spark.avro").load(destDir.toString)
    val actualRows: Array[Row] = df.sort("description").collect

    val expectedRows = Array(
      Row("thing-1", 1, "comment-1", "new column"),
      Row("thing-2", 2, "comment-2", "new column"),
      Row("thing-3", 3, "comment-3", "new column"),
      Row("thing-4", 4, "comment-4", "new column"),
      Row("thing-5", 5, "comment-5", "new column")
    )
    assertResult(expectedRows)(actualRows)

    FileUtils.deleteDirectory(testDir.toFile)

    spark.stop()
  }

  it("moves an Avro data set") {

    // Given
    val spark = localSparkSession
    val testDir = Files.createTempDirectory("testdata")
    val sourceDir = Files.createDirectory(Paths.get(testDir.toString, "source"))
    val destDir = Files.createDirectory(Paths.get(testDir.toString, "dest"))

    val testDf: DataFrame = createTestDataFrame(spark)
    testDf.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(sourceDir.toString)

    val etlJob:EtlJob = new EtlJob(SimpleAvroReader(spark), SimpleAvroWriter())

    // When
    etlJob.transform(sourceDir.toString, destDir.toString)

    // Then
    val df = localSparkSession.read.format("com.databricks.spark.avro").load(destDir.toString)
    val actualRows: Array[Row] = df.sort("description").collect

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

  it("reads an Avro data set with schema") {

    // Given
    val spark = localSparkSession
    val testDir = Files.createTempDirectory("testdata")
    val sourceDir = Files.createDirectory(Paths.get(testDir.toString, "source"))
    val destDir = Files.createDirectory(Paths.get(testDir.toString, "dest"))

    val testDf: DataFrame = createTestDataFrame(spark)
    testDf.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(sourceDir.toString)

    val schema = StructType(Seq(
      StructField("description", StringType, nullable = true),
      StructField("numThings", IntegerType, nullable = true)
    ))

    val etlJob:EtlJob = new EtlJob(SimpleAvroReader(spark).withSchema(schema), SimpleAvroWriter())

    // When
    etlJob.transform(sourceDir.toString, destDir.toString)

    // Then
    val df = localSparkSession.read.format("com.databricks.spark.avro").load(destDir.toString)
    val actualRows: Array[Row] = df.sort("description").collect

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

  it("TODO: has not been done yet") {
    /*
    TODO
    - overwrite existing data
    - Copier?
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
