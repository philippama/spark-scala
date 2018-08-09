package pm.spark.etl.jsontoavro

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class ModelToAvroTest extends FunSpec {

  describe("generate SQL schema") {
    it("generates SQL schema from class") {
      // Given

      // When
      val schema = ScalaReflection.schemaFor[TestClass].dataType.asInstanceOf[StructType]

      // Then
      val expectedSchema = StructType(Seq(
        StructField("string", StringType, nullable = true),
        StructField("integer", IntegerType, nullable = false))
      )
      assertResult(expectedSchema)(schema)
    }

    it("generates SQL schema from a deep class") {
      // Given

      // When
      val schema = ScalaReflection.schemaFor[TestClassLevel1].dataType.asInstanceOf[StructType]

      // Then
      val testClassSchema = StructType(Seq(
        StructField("string", StringType, nullable = true),
        StructField("integer", IntegerType, nullable = false))
      )
      val expectedSchema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("testClass", testClassSchema, nullable = true),
        StructField("testClasses", ArrayType(testClassSchema))
      ))
      assertResult(expectedSchema)(schema)
    }

  }

  describe("generate Avro schema") {
    it("generates Avro schema from class") {
      // Given

      // When
      val schema = ScalaReflection.schemaFor[TestClass].dataType.asInstanceOf[StructType]

      import com.databricks.spark.avro.SchemaConverters
      val namespace = "data-engineering"
      val builder = SchemaBuilder.record("testclass").namespace(namespace)
      val avroSchema = SchemaConverters.convertStructToAvro(schema, builder, namespace)

      println(avroSchema)

      // Then
      val actualSchema = avroSchema.toString
      val expectedSchema = "{\"type\":\"record\",\"name\":\"testclass\",\"namespace\":\"data-engineering\",\"fields\":[{\"name\":\"string\",\"type\":[\"string\",\"null\"]},{\"name\":\"integer\",\"type\":\"int\"}]}"
      assertResult(expectedSchema)(actualSchema)
    }

    it("generates Avro schema from a deep class") {
      // Given

      // When
      val schema = ScalaReflection.schemaFor[TestClassLevel1].dataType.asInstanceOf[StructType]

      import com.databricks.spark.avro.SchemaConverters
      val namespace = "data-engineering"
      val builder = SchemaBuilder.record("testclass").namespace(namespace)
      val avroSchema = SchemaConverters.convertStructToAvro(schema, builder, namespace)

      println(avroSchema)

      // Then
      val actualSchema = avroSchema.toString
      val expectedSchema = "{\"type\":\"record\",\"name\":\"testclass\",\"namespace\":\"data-engineering\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"testClass\",\"type\":[{\"type\":\"record\",\"name\":\"testClass\",\"namespace\":\"data-engineering.testClass\",\"fields\":[{\"name\":\"string\",\"type\":[\"string\",\"null\"]},{\"name\":\"integer\",\"type\":\"int\"}]},\"null\"]},{\"name\":\"testClasses\",\"type\":[{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"testClasses\",\"namespace\":\"data-engineering.testClasses\",\"fields\":[{\"name\":\"string\",\"type\":[\"string\",\"null\"]},{\"name\":\"integer\",\"type\":\"int\"}]},\"null\"]},\"null\"]}]}"
      assertResult(expectedSchema)(actualSchema)
    }
  }
}

case class TestClass(string: String, integer: Int)
//case class TestClassLevel1(name: String, testClass: TestClass)
case class TestClassLevel1(name: String, testClass: TestClass, testClasses: Seq[TestClass])
