package pm.spark.converters

import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class StructTypeToStringSpec extends FunSpec {

  describe("StructTypeToStructType") {

    it("generates StructType definition string for a flat schema with one type") {
      val schema = StructType(Seq(
        StructField("aString", StringType, nullable = false)
      ))

      println(s"Generating StructType string for $schema")

      val actual = StructTypeToString(schema).buildStructTypeString

      val expected = "StructType(Seq(StructField(\"aString\", StringType, nullable = false)))"

      assert(actual == expected)
    }

    it("generates StructType definition string for a flat schema with simple types") {
      val schema = StructType(Seq(
        StructField("aByte", ByteType, nullable = false),
        StructField("aShort", ShortType, nullable = false),
        StructField("anInteger", IntegerType, nullable = false),
        StructField("aLong", LongType, nullable = false),
        StructField("aFloat", FloatType, nullable = false),
        StructField("aDouble", DoubleType, nullable = false),
        StructField("aString", StringType, nullable = false),
        StructField("aBoolean", BooleanType, nullable = false),
        StructField("aTimestamp", TimestampType, nullable = false),
        StructField("aDate", DateType, nullable = false)
      ))

      val actual = StructTypeToString(schema).buildStructTypeString

      val expected = "StructType(Seq(StructField(\"aByte\", ByteType, nullable = false), StructField(\"aShort\", ShortType, nullable = false), StructField(\"anInteger\", IntegerType, nullable = false), StructField(\"aLong\", LongType, nullable = false), StructField(\"aFloat\", FloatType, nullable = false), StructField(\"aDouble\", DoubleType, nullable = false), StructField(\"aString\", StringType, nullable = false), StructField(\"aBoolean\", BooleanType, nullable = false), StructField(\"aTimestamp\", TimestampType, nullable = false), StructField(\"aDate\", DateType, nullable = false)))"

      assert(actual == expected)
    }

    it("generates StructType definition string for a flat schema with an array") {
      val schema = StructType(Seq(
        StructField("anIntegerArray", ArrayType(IntegerType, containsNull = false), nullable = false),
        StructField("aStringArray", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("aString", StringType, nullable = false)
      ))

      val actual = StructTypeToString(schema).buildStructTypeString

      val expected = "StructType(Seq(StructField(\"anIntegerArray\", ArrayType(IntegerType, containsNull = false), nullable = false), StructField(\"aStringArray\", ArrayType(StringType, containsNull = false), nullable = false), StructField(\"aString\", StringType, nullable = false)))"

      assert(actual == expected)
    }

    it("generates StructType definition string for a flat schema with other stuff") {
      val schema = StructType(Seq(
        //        StructField("aDecimal", DecimalType, nullable = false),
        //        StructField("aBinary", BinaryType, nullable = false),
        //        StructField("aMap", MapType, nullable = false),
        StructField("aString", StringType, nullable = false)
      ))

      //{"name":"previousSearches","type":["null",{"type":"array","items":{"type":"record","name":"PreviousSearch","fields":[{"name":"performed","type":["null","string"]},{"name":"typeOfBusiness","type":["null","string"]}]}}]}
      //schema.avsc: SchemaType(StructType(StructField(key,StringType,true), StructField(eventSource,StringType,true), StructField(eventTime,StringType,true), StructField(request,StructType(StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(did,StringType,true), StructField(ursId,StringType,true), StructField(user,StringType,true), StructField(sourceSystem,StringType,true), StructField(stockItemId,StringType,true)),true), StructField(status,StringType,true), StructField(errorMessage,StringType,true), StructField(dataSource,StringType,true), StructField(vehicleCheck,StructType(StructField(id,StringType,true), StructField(performed,StringType,true), StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(engineNumber,StringType,true), StructField(dvlaMake,StringType,true), StructField(dvlaModel,StringType,true), StructField(yearOfManufacture,IntegerType,true), StructField(registrationDate,StringType,true), StructField(insuranceWriteoffType,StringType,true), StructField(scrappedDate,StringType,true), StructField(exportedDate,StringType,true), StructField(co2Emissions,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(stolen,BooleanType,true), StructField(scrapped,BooleanType,true), StructField(exported,BooleanType,true), StructField(imported,BooleanType,true), StructField(highRisk,BooleanType,true), StructField(privateFinance,BooleanType,true), StructField(tradeFinance,BooleanType,true), StructField(mileageDiscrepancy,BooleanType,true), StructField(registrationChanged,BooleanType,true), StructField(colourChanged,BooleanType,true), StructField(policeStolenMarker,StructType(StructField(recordedDate,StringType,true), StructField(policeForce,StringType,true), StructField(telephoneNumber,StringType,true)),true), StructField(motTests,ArrayType(StructType(StructField(motTestNumber,StringType,true), StructField(performed,StringType,true), StructField(testResult,StringType,true), StructField(expiryDate,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(motTestItems,ArrayType(StructType(StructField(type,StringType,true), StructField(notes,StringType,true)),false),true)),false),true), StructField(financeAgreements,ArrayType(StructType(StructField(agreementId,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(startDate,StringType,true), StructField(term,IntegerType,true), StructField(type,StringType,true)),false),true), StructField(v5cs,ArrayType(StructType(StructField(issuedDate,StringType,true)),false),true), StructField(plateChanges,ArrayType(StructType(StructField(currentRegistration,StringType,true), StructField(previousRegistration,StringType,true), StructField(startDate,StringType,true)),false),true), StructField(insuranceWriteoffs,ArrayType(StructType(StructField(type,StringType,true), StructField(lossDate,StringType,true), StructField(removedDate,StringType,true)),false),true), StructField(highRiskMarkers,ArrayType(StructType(StructField(startDate,StringType,true), StructField(type,StringType,true), StructField(extraInfo,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(reference,StringType,true)),false),true), StructField(keeperChanges,ArrayType(StructType(StructField(startDate,StringType,true)),false),true), StructField(colourChanges,ArrayType(StructType(StructField(startDate,StringType,true), StructField(previousColour,StringType,true)),false),true), StructField(odometerReadings,ArrayType(StructType(StructField(performed,StringType,true), StructField(source,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true)),false),true),StructField(previousSearches,ArrayType(StructType(StructField(performed,StringType,true), StructField(typeOfBusiness,StringType,true)),false),true)),true)),false)

      //TODO
      pending

      assert(schema.length == 1)
      println(schema)

      val actual = StructTypeToString(schema).buildStructTypeString

      val expected = "StructType definition string GeneratedClass(" +
        "aBoolean: Boolean, " +
        "anArray: Seq[], " +
        "aDate: java.sql.Date, " +
        "aString: String" +
        ")"

      assert(actual == expected)
    }

    it("generates StructType definition string for a schema with nested non-standard classes") {
      val simpleSchema = StructType(Seq(
        StructField("aString", StringType, nullable = false),
        StructField("anInteger", IntegerType, nullable = false))
      )
      val complexSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("testClass", simpleSchema, nullable = false),
        StructField("testClasses", ArrayType(simpleSchema, containsNull = false), nullable = false)
      ))

      val actual = StructTypeToString(complexSchema).buildStructTypeString

      val expected = "StructType(Seq(StructField(\"name\", StringType, nullable = false), StructField(\"testClass\", StructType(Seq(StructField(\"aString\", StringType, nullable = false), StructField(\"anInteger\", IntegerType, nullable = false))), nullable = false), StructField(\"testClasses\", ArrayType(StructType(Seq(StructField(\"aString\", StringType, nullable = false), StructField(\"anInteger\", IntegerType, nullable = false))), containsNull = false), nullable = false)))"

      assert(actual == expected)
    }

    it("generates StructType definition string for nullable types") {
      val simpleSchema = StructType(Seq(
        StructField("aString", StringType, nullable = true),
        StructField("anInteger", IntegerType, nullable = true))
      )
      val schema = StructType(Seq(
        StructField("aByte", ByteType, nullable = true),
        StructField("aShort", ShortType, nullable = true),
        StructField("anInteger", IntegerType, nullable = true),
        StructField("aLong", LongType, nullable = true),
        StructField("aFloat", FloatType, nullable = true),
        StructField("aDouble", DoubleType, nullable = true),
        StructField("aString", StringType, nullable = true),
        StructField("aBoolean", BooleanType, nullable = true),
        StructField("aTimestamp", TimestampType, nullable = true),
        StructField("aDate", DateType, nullable = true),
        StructField("anIntegerArray", ArrayType(IntegerType, containsNull = true), nullable = true),
        StructField("testClass", simpleSchema, nullable = true),
        StructField("testClasses", ArrayType(simpleSchema, containsNull = true), nullable = true)
      ))

      val actual = StructTypeToString(schema).buildStructTypeString

      val expected = "StructType(Seq(StructField(\"aByte\", ByteType, nullable = true), StructField(\"aShort\", ShortType, nullable = true), StructField(\"anInteger\", IntegerType, nullable = true), StructField(\"aLong\", LongType, nullable = true), StructField(\"aFloat\", FloatType, nullable = true), StructField(\"aDouble\", DoubleType, nullable = true), StructField(\"aString\", StringType, nullable = true), StructField(\"aBoolean\", BooleanType, nullable = true), StructField(\"aTimestamp\", TimestampType, nullable = true), StructField(\"aDate\", DateType, nullable = true), StructField(\"anIntegerArray\", ArrayType(IntegerType, containsNull = true), nullable = true), StructField(\"testClass\", StructType(Seq(StructField(\"aString\", StringType, nullable = true), StructField(\"anInteger\", IntegerType, nullable = true))), nullable = true), StructField(\"testClasses\", ArrayType(StructType(Seq(StructField(\"aString\", StringType, nullable = true), StructField(\"anInteger\", IntegerType, nullable = true))), containsNull = true), nullable = true)))"

      assert(actual == expected)
    }
  }
}
