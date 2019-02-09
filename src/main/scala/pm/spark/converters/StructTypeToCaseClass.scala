package pm.spark.converters

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.types._

class StructTypeToCaseClass(schema: StructType, className: String) {

  private val classCount = new AtomicInteger(1)

  def buildCaseClassString: String = buildCaseClassString(schema, className)

  private def buildCaseClassString(structType: StructType, classBaseName: String): String = {
    val fields = structType.fields.map(field => {
      s"${field.name}: ${dataTypeToClass(field.dataType)}"
    })

    val caseClassString = fields.mkString(s"case class $classBaseName(", ", ", ")")
    println(s"Generated: [$caseClassString]")
    caseClassString
  }

  private def dataTypeToClass(dataType: DataType): String = {
    dataType match {
      case _: ByteType => "Byte"
      case _: ShortType => "Short"
      case _: IntegerType => "Int"
      case _: LongType => "Long"
      case _: FloatType => "Float"
      case _: DoubleType => "Double"
      case _: StringType => "String"
      case _: BooleanType => "Boolean"
      case _: TimestampType => "java.sql.Timestamp"
      case _: DateType => "java.sql.Date"
      case d: ArrayType => buildArrayType(d)
      case d: StructType => buildStructType(d)
      case t => throw new UnsupportedOperationException(s"DataType [$t] is not supported")
    }
  }

  private def buildArrayType(field: ArrayType) = {
    s"Seq[${dataTypeToClass(field.elementType)}]"
  }

  private def buildStructType(field: StructType): String = {
    val newClassName = s"$className${classCount.getAndIncrement}"
    buildCaseClassString(field, newClassName)
    newClassName
  }

}

object StructTypeToCaseClass{
  def apply(schema: StructType, className: String = "GeneratedClass") =
    new StructTypeToCaseClass(schema, className = className)
}