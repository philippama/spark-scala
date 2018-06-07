package pm.spark.etl

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, types}
//import org.scalatest.{FunSpec}
import org.scalatest.{BeforeAndAfter, /*BeforeAndAfterAll,*/ FunSpec}

//class ExtractTest extends FunSpec /*with BeforeAndAfterAll*/ with BeforeAndAfter {
class ExtractTest extends FunSpec {

//TODO: beforeAll, afterAll
//  var sparkSession: SparkSession = _
//  var extract: Extract = _

//  override def beforeAll {
//    sparkSession = SparkSession
//      .builder()
//      .appName("Spark session for testing")
//      //    .config("spark.some.config.option", "some-value")
//      .getOrCreate()
//  }
//
//  override def afterAll {
//    sparkSession.stop()
//  }

//  before {
//    setUp
//  }
//
//  after {
//    tearDown
//  }

  describe("loads data from a JSON file") {
    it("loads data without a schema") {

      // Given
      val localSpark = localSparkSession
      val extract = new Extract(localSpark)

      val df = extract.fromJson(getResourcePath("things.json"))

      // When
      df.printSchema()

      // Then
      val expectedSchema = StructType(Seq(
        StructField("description", StringType, nullable = true),
        StructField("numThings", LongType, nullable = true)
      ))
      assertResult(expectedSchema)(df.schema)

      val actualRows: Array[Row] = df.sort("description").collect
      assertResult(5)(actualRows.length)

      val expectedRows = Array(
        Row("thing-1", 1L),
        Row("thing-2", 2L),
        Row("thing-3", 3L),
        Row("thing-4", 4L),
        Row("thing-5", 5L)
      )
      assertResult(expectedRows)(actualRows)

      localSpark.stop()
    }

    it("loads data with a schema") {

      // Given
      val localSpark = localSparkSession
      val extract = new Extract(localSpark)

      val testSchema = StructType(Seq(
        StructField("description", StringType, nullable = true),
        StructField("numThings", IntegerType, nullable = true)
      ))

      // When
      val df = extract.fromJson(getResourcePath("things.json"), testSchema)

      df.printSchema()

      // Then
      assertResult(testSchema)(df.schema)

      val actualRows: Array[Row] = df.sort("description").collect
      assertResult(5)(actualRows.length)

      val expectedRows = Array(
        Row("thing-1", 1),
        Row("thing-2", 2),
        Row("thing-3", 3),
        Row("thing-4", 4),
        Row("thing-5", 5)
      )
      assertResult(expectedRows)(actualRows)

      localSpark.stop()
    }
  }

  describe("loads data from an Avro file") {
    it("loads data without a schema") {

      // Given
      var localSpark = localSparkSession
      var extract = new Extract(localSpark)

      // When
      val df = extract.fromAvro(getResourcePath("things.avro"))

      // Then
      df.columns
      val numRows = df.count
      assertResult(5)(numRows)

      df.printSchema()
      df.schema.catalogString
      df.schema.treeString

      localSpark.stop()
    }
  }

  private def localSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("fes-lucene-test")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.port", 7077)
      .getOrCreate()
  }

  private def getResourcePath(fileName: String) = {
    this.getClass.getResource(fileName).getPath
  }
}
