package pm.spark.s3

class Keys(s3Keys: List[String]) {
  def prefixesVsFileNames: Map[String, Set[String]] = {
    s3Keys.foldLeft(Map[String, Set[String]]()) {
      (acc, s3Key) => {
        val i = s3Key.lastIndexOf("/")
        val prefix = s3Key.substring(0, i)
        val fileName = s3Key.substring(i + 1)
        acc + (prefix -> (acc.getOrElse(prefix, Set.empty) + fileName))
      }
    }
  }
}

object Keys {
  def apply(keys: List[String]): Keys = {
    new Keys(keys)
  }
}