package pm.spark.etl.replace

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class DataSetTransformer(spark: SparkSession, dataPath: String, tempDataPath: String, frameToFrame: DataFrame => DataFrame) {
  def transform(): Unit = {
    val sourceDf = spark.read.format("com.databricks.spark.avro").load(dataPath)

    sourceDf.transform(frameToFrame)
      //TODO: This could be in a DataSetWriter (no re-partitioning here)
      .write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(tempDataPath)

    //TODO: This could be in a DataSetMover
    val df = spark.read.format("com.databricks.spark.avro").load(tempDataPath)

    //TODO: This could be in a DataSetWriter that handles re-partitioning etc: DataSetWriter.withPartitionCalculator(...)
    df.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(dataPath)
  }

  def transform(df: DataFrame): DataFrame = {
    df.transform(frameToFrame)
  }
}

object DataSetTransformer {
  def apply(spark: SparkSession, dataPath: String, tempDataPath: String, frameToFrame: DataFrame => DataFrame): DataSetTransformer = {
    new DataSetTransformer(spark, dataPath, tempDataPath, frameToFrame)
  }
}