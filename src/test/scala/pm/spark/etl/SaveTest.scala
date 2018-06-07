package pm.spark.etl

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

case class TestClass(description: String, numThings: Int)

class SaveTest extends FunSpec {

  //TODO: Before and After

  describe("saves data") {
    it("saves data to a JSON file") {

      // Given
      val localSpark = localSparkSession
      val save = new Save(localSpark)

      val testDf: DataFrame = createTestDataFrame(localSpark)
      val tempDir = Files.createTempDirectory("savetest")

      // When
      save.asJson(testDf, s"${tempDir.toString}")

      // Then
      val extract = new Extract(localSpark)
      val actualDf = extract.fromJson(tempDir.toString)
      assertResult(testDf.collect)(actualDf.collect())

      localSpark.stop()
      FileUtils.deleteDirectory(tempDir.toFile)
    }

    it("saves data to an Avro file") {

      // Given
      val localSpark = localSparkSession
      val save = new Save(localSpark)

      val testDf: DataFrame = createTestDataFrame(localSpark)
      val tempDir = Files.createTempDirectory("savetest")

      // When
      save.asAvro(testDf, s"${tempDir.toString}")

      // Then
      val extract = new Extract(localSpark)
      val actualDf = extract.fromAvro(tempDir.toString)
      assertResult(testDf.schema)(actualDf.schema)
      assertResult(testDf.collect)(actualDf.collect())

      localSpark.stop()
      FileUtils.deleteDirectory(tempDir.toFile)
    }
  }

//TODO: refactor out a trait for this - or use SharedSparkContext from spark-testing-base?
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
      StructField("numThings", IntegerType, nullable = true)
    ))

    val testRows = Array(
      Row("thing-1", 1),
      Row("thing-2", 2),
      Row("thing-3", 3),
      Row("thing-4", 4),
      Row("thing-5", 5)
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(testRows),
      StructType(testSchema)
    )
  }

}

