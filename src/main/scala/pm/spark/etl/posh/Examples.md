# Examples
Some of these examples use classes that extend DataFrameTransformer to do
data transformation. They are shown at the bottom of this page.

## ETL from one location to another

### To copy a data set
```
val etlJob = EtlJob(SimpleAvroReader(spark), SimpleAvroWriter())
etlJob.run(sourcePath, destPath)
```
### To copy a data set using a read schema
Providing a schema is optional.
```
val etlJob = EtlJob(SimpleAvroReader(spark).withSchema(schema), SimpleAvroWriter())
etlJob.run(sourcePath, destPath)
```
### To transform a data set
This needs a class that extends DataFrameTransformer to do the transformation.
```
val etlJob:EtlJob = EtlJob(SimpleAvroReader(spark), SimpleAvroWriter())
  .withTransformer(new Repartitioner(4))
etlJob.run(sourcePath, destPath)
```
## Altering a data set in situ
These need a location to store temporary data.
### To change data format
```
val dataAdjuster = DataAdjuster(spark, SimpleAvroReader(spark).withSchema(schema), SimpleParquetWriter())
dataAdjuster.run(sourcePath, tempPath)
```
### To change to a different but compatible schema
This can be used, for example, to drop a column.
```
val dataAdjuster = DataAdjuster(spark, SimpleAvroReader(spark).withSchema(schema), SimpleAvroWriter())
dataAdjuster.run(sourcePath, tempPath)
```
### To transform a data set
Data sets can be transformed after reading and/or before writing.

```
val dataAdjuster = DataAdjuster(spark, SimpleAvroReader(spark), SimpleAvroWriter())
  .withPostReadTransformer(new NewColumnTransformer())
  .withPreWriteTransformer(new Repartitioner(5))
dataAdjuster.run(sourcePath, tempPath)
```
## Simple example transformers
```
class NewColumnTransformer extends DataFrameTransformer {
  def transform(df: DataFrame): DataFrame = df.withColumn("newColumn", lit("new column"))
}

class Repartitioner(numPartitions: Int) extends DataFrameTransformer {
  def transform(df: DataFrame): DataFrame = df.coalesce(numPartitions)
}
```
