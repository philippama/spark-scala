package pm.spark.s3

import org.scalatest.FunSpec

class KeysTest extends FunSpec {
  val testKeys = List(
    "topics/vehicle-metrics/year=2018/month=05/day=15/hour=10/pm-json-10-01.json",
    "topics/vehicle-metrics/year=2018/month=05/day=15/hour=11/pm-json-11-01.json",
    "topics/vehicle-metrics/year=2018/month=05/day=15/hour=11/pm-json-11-02.json",
    "topics/vehicle-metrics/year=2018/month=05/day=15/hour=11/pm-json-11-03.json",
    "external/pm-csv/somewhere/pm-csv-01.csv"
  )

  describe("has keys") {
    it("parses key into prefix and value") {
      val keys = Keys(testKeys)
      val prefixesAndFiles: Map[String, Set[String]] = keys.prefixesVsFileNames

      assertResult(3)(prefixesAndFiles.size)
      assertResult(Set("pm-json-10-01.json"))(prefixesAndFiles("topics/vehicle-metrics/year=2018/month=05/day=15/hour=10"))
      assertResult(Set("pm-json-11-01.json", "pm-json-11-02.json", "pm-json-11-03.json"))(prefixesAndFiles("topics/vehicle-metrics/year=2018/month=05/day=15/hour=11"))
      assertResult(Set("pm-csv-01.csv"))(prefixesAndFiles("external/pm-csv/somewhere"))
    }
  }
}
