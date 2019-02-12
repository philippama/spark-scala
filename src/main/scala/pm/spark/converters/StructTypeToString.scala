package pm.spark.converters

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.types._

case class StructTypeToString(schema: StructType) {

  def buildStructTypeString: String = {
    val structTypeString = generateStructTypeString(schema)
    println("Generated:")
    println(structTypeString)
    structTypeString
  }

  private def generateStructTypeString(structType: StructType): String = {
    val fields = structType.fields.map(field => {
      fieldToStructField(field)
    })

    fields.mkString(s"StructType(Seq(", ", ", "))").replace('\'', '"')
  }

  private def fieldToStructField(field: StructField) = {
    s"StructField('${field.name}', ${dataTypeToString(field.dataType)}, nullable = ${field.nullable})"
  }

  private def dataTypeToString(dataType: DataType): String = {
    //TODO: or reflection?
    dataType match {
      case _: ByteType => "ByteType"
      case _: ShortType => "ShortType"
      case _: IntegerType => "IntegerType"
      case _: LongType => "LongType"
      case _: FloatType => "FloatType"
      case _: DoubleType => "DoubleType"
      case _: StringType => "StringType"
      case _: BooleanType => "BooleanType"
      case _: TimestampType => "TimestampType"
      case _: DateType => "DateType"
      case d: StructType => buildStructType(d)
      case d: ArrayType => buildArrayType(d)
      case t => throw new UnsupportedOperationException(s"DataType [$t] is not supported")
    }
  }

  private def buildArrayType(field: ArrayType) = {
    s"ArrayType(${dataTypeToString(field.elementType)}, containsNull = ${field.containsNull})"
  }

  private def buildStructType(field: StructType) = {
    generateStructTypeString(field)
  }
}

