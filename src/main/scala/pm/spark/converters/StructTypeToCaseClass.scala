package pm.spark.converters

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.types._

class StructTypeToCaseClass(schema: StructType, className: String) {

  //TODO: Don't be mutating!
  private val classCount = new AtomicInteger(1)
  private var generatedCaseClasses = Seq[String]()

  def buildCaseClassString: String = {
    generateCaseClassString(schema, className)
    val caseClassString = generatedCaseClasses.mkString("\n")
    println("Generated case class(es):")
    println(caseClassString)
    caseClassString
  }

  private def generateCaseClassString(structType: StructType, classBaseName: String): Unit = {
    val fields = structType.fields.map(field => {
      s"${field.name}: ${dataTypeToClass(field.dataType, field.nullable)}"
    })

    val caseClassString = fields.mkString(s"case class $classBaseName(", ", ", ")")
    generatedCaseClasses = generatedCaseClasses :+ caseClassString
  }

  private def dataTypeToClass(dataType: DataType, isNullable: Boolean): String = {
    val dataTypeString = dataType match {
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
      case d: StructType => buildStructType(d)
      case d: ArrayType => buildArrayType(d)
      case t => throw new UnsupportedOperationException(s"DataType [$t] is not supported")
    }
    optionIfNullable(dataTypeString, isNullable)
  }

  private def optionIfNullable(dataType: String, isNullable: Boolean) = {
    if (isNullable) {
      s"Option[$dataType]"
    }
    else {
      dataType
    }
  }

  private def buildArrayType(field: ArrayType) = {
    s"Seq[${dataTypeToClass(field.elementType, field.containsNull)}]"
  }

  private def buildStructType(field: StructType): String = {
    val newClassName = s"$className${classCount.getAndIncrement}"
    generateCaseClassString(field, newClassName)
    newClassName
  }
}

object StructTypeToCaseClass{
  def apply(schema: StructType, className: String = "GeneratedClass") =
    new StructTypeToCaseClass(schema, className = className)
}