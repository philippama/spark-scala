package pm.spark.etl

import org.apache.spark.sql.SparkSession
//import org.scalatest.{FunSpec}
import org.scalatest.{BeforeAndAfter, /*BeforeAndAfterAll,*/ FunSpec}

//class ExtractTest extends FunSpec /*with BeforeAndAfterAll*/ with BeforeAndAfter {
class ExtractTest extends FunSpec {

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

  describe("loads data") {
    it("loads data from a JSON file") {

      var localSpark = localSparkSession
      var extract = new Extract(localSpark)

      val df = extract.fromJson(getResourcePath("derivative-data-1-5.json"))

      df.columns
      val numRows = df.count
      assertResult(5)(numRows)

//      df.printSchema()

      localSpark.stop()
    }
  }

  describe("loads data") {
    it("loads data from an Avro file") {
      var localSpark = localSparkSession
      var extract = new Extract(localSpark)

      val df = extract.fromAvro(getResourcePath("derivative-data-1-5.avro"))

      df.columns
      val numRows = df.count
      assertResult(5)(numRows)

      df.printSchema()

      localSpark.stop()
    }
  }

  private def localSparkSession = {
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
