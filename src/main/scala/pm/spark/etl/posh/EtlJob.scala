package pm.spark.etl.posh

class EtlJob(reader: SparkReader, writer: SparkWriter, dataFrameTransformer: DataFrameTransformer = IdentityTransformer()) {

  private var transformer: DataFrameTransformer = dataFrameTransformer

  def withTransformer(transformer: DataFrameTransformer): EtlJob = {
    this.transformer = transformer
    this
  }

  def run(sourcePath: String, destPath: String): Unit = {
    val df = reader.extract(sourcePath)
    writer.load(transformer.transform(df), destPath)
  }
}

object EtlJob {
  def apply(reader: SparkReader, writer: SparkWriter) = new EtlJob(reader, writer)
}
