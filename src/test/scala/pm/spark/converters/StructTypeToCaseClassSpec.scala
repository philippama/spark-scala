package pm.spark.converters

import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.avro.SchemaConverters.SchemaType
import org.apache.avro.Schema.Parser
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class StructTypeToCaseClassSpec extends FunSpec {

  describe("generates case class") {
    it("generates case class for an empty schema") {
      val schema = StructType(Seq())

      val actual = StructTypeToCaseClass(schema, "EmptyClass").buildCaseClassString

      val expected = "case class EmptyClass()"

      assert(actual == expected)
    }

    it("generates case class for a flat schema with simple types") {
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

      val actual = StructTypeToCaseClass(schema).buildCaseClassString

      val expected = "case class GeneratedClass(" +
        "aByte: Byte, " +
        "aShort: Short, " +
        "anInteger: Int, " +
        "aLong: Long, " +
        "aFloat: Float, " +
        "aDouble: Double, " +
        "aString: String, " +
        "aBoolean: Boolean, " +
        "aTimestamp: java.sql.Timestamp, " +
        "aDate: java.sql.Date" +
        ")"

      assert(actual == expected)
    }

    it("generates case class for a flat schema with an array") {
      val schema = StructType(Seq(
        StructField("anIntegerArray", ArrayType(IntegerType, containsNull = false), nullable = false),
        StructField("aStringArray", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("aString", StringType, nullable = false)
      ))

      //{"name":"previousSearches","type":["null",{"type":"array","items":{"type":"record","name":"PreviousSearch","fields":[{"name":"performed","type":["null","string"]},{"name":"typeOfBusiness","type":["null","string"]}]}}]}
      //schema.avsc: SchemaType(StructType(StructField(key,StringType,true), StructField(eventSource,StringType,true), StructField(eventTime,StringType,true), StructField(request,StructType(StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(did,StringType,true), StructField(ursId,StringType,true), StructField(user,StringType,true), StructField(sourceSystem,StringType,true), StructField(stockItemId,StringType,true)),true), StructField(status,StringType,true), StructField(errorMessage,StringType,true), StructField(dataSource,StringType,true), StructField(vehicleCheck,StructType(StructField(id,StringType,true), StructField(performed,StringType,true), StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(engineNumber,StringType,true), StructField(dvlaMake,StringType,true), StructField(dvlaModel,StringType,true), StructField(yearOfManufacture,IntegerType,true), StructField(registrationDate,StringType,true), StructField(insuranceWriteoffType,StringType,true), StructField(scrappedDate,StringType,true), StructField(exportedDate,StringType,true), StructField(co2Emissions,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(stolen,BooleanType,true), StructField(scrapped,BooleanType,true), StructField(exported,BooleanType,true), StructField(imported,BooleanType,true), StructField(highRisk,BooleanType,true), StructField(privateFinance,BooleanType,true), StructField(tradeFinance,BooleanType,true), StructField(mileageDiscrepancy,BooleanType,true), StructField(registrationChanged,BooleanType,true), StructField(colourChanged,BooleanType,true), StructField(policeStolenMarker,StructType(StructField(recordedDate,StringType,true), StructField(policeForce,StringType,true), StructField(telephoneNumber,StringType,true)),true), StructField(motTests,ArrayType(StructType(StructField(motTestNumber,StringType,true), StructField(performed,StringType,true), StructField(testResult,StringType,true), StructField(expiryDate,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(motTestItems,ArrayType(StructType(StructField(type,StringType,true), StructField(notes,StringType,true)),false),true)),false),true), StructField(financeAgreements,ArrayType(StructType(StructField(agreementId,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(startDate,StringType,true), StructField(term,IntegerType,true), StructField(type,StringType,true)),false),true), StructField(v5cs,ArrayType(StructType(StructField(issuedDate,StringType,true)),false),true), StructField(plateChanges,ArrayType(StructType(StructField(currentRegistration,StringType,true), StructField(previousRegistration,StringType,true), StructField(startDate,StringType,true)),false),true), StructField(insuranceWriteoffs,ArrayType(StructType(StructField(type,StringType,true), StructField(lossDate,StringType,true), StructField(removedDate,StringType,true)),false),true), StructField(highRiskMarkers,ArrayType(StructType(StructField(startDate,StringType,true), StructField(type,StringType,true), StructField(extraInfo,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(reference,StringType,true)),false),true), StructField(keeperChanges,ArrayType(StructType(StructField(startDate,StringType,true)),false),true), StructField(colourChanges,ArrayType(StructType(StructField(startDate,StringType,true), StructField(previousColour,StringType,true)),false),true), StructField(odometerReadings,ArrayType(StructType(StructField(performed,StringType,true), StructField(source,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true)),false),true),
      // StructField(previousSearches,ArrayType(StructType(StructField(performed,StringType,true), StructField(typeOfBusiness,StringType,true)),false)
      // ,true)),true)),false)

      val actual = StructTypeToCaseClass(schema).buildCaseClassString

      val expected = "case class GeneratedClass(" +
        "anIntegerArray: Seq[Int], " +
        "aStringArray: Seq[String], " +
        "aString: String" +
        ")"

      assert(actual == expected)
    }

    it("generates case class for a flat schema with other stuff") {
      val schema = StructType(Seq(
        //        StructField("aDecimal", DecimalType, nullable = false),
        //        StructField("aBinary", BinaryType, nullable = false),
        //        StructField("aMap", MapType, nullable = false),
        StructField("aString", StringType, nullable = false)
      ))

      //{"name":"previousSearches","type":["null",{"type":"array","items":{"type":"record","name":"PreviousSearch","fields":[{"name":"performed","type":["null","string"]},{"name":"typeOfBusiness","type":["null","string"]}]}}]}
      //schema.avsc: SchemaType(StructType(StructField(key,StringType,true), StructField(eventSource,StringType,true), StructField(eventTime,StringType,true), StructField(request,StructType(StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(did,StringType,true), StructField(ursId,StringType,true), StructField(user,StringType,true), StructField(sourceSystem,StringType,true), StructField(stockItemId,StringType,true)),true), StructField(status,StringType,true), StructField(errorMessage,StringType,true), StructField(dataSource,StringType,true), StructField(vehicleCheck,StructType(StructField(id,StringType,true), StructField(performed,StringType,true), StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(engineNumber,StringType,true), StructField(dvlaMake,StringType,true), StructField(dvlaModel,StringType,true), StructField(yearOfManufacture,IntegerType,true), StructField(registrationDate,StringType,true), StructField(insuranceWriteoffType,StringType,true), StructField(scrappedDate,StringType,true), StructField(exportedDate,StringType,true), StructField(co2Emissions,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(stolen,BooleanType,true), StructField(scrapped,BooleanType,true), StructField(exported,BooleanType,true), StructField(imported,BooleanType,true), StructField(highRisk,BooleanType,true), StructField(privateFinance,BooleanType,true), StructField(tradeFinance,BooleanType,true), StructField(mileageDiscrepancy,BooleanType,true), StructField(registrationChanged,BooleanType,true), StructField(colourChanged,BooleanType,true), StructField(policeStolenMarker,StructType(StructField(recordedDate,StringType,true), StructField(policeForce,StringType,true), StructField(telephoneNumber,StringType,true)),true), StructField(motTests,ArrayType(StructType(StructField(motTestNumber,StringType,true), StructField(performed,StringType,true), StructField(testResult,StringType,true), StructField(expiryDate,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(motTestItems,ArrayType(StructType(StructField(type,StringType,true), StructField(notes,StringType,true)),false),true)),false),true), StructField(financeAgreements,ArrayType(StructType(StructField(agreementId,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(startDate,StringType,true), StructField(term,IntegerType,true), StructField(type,StringType,true)),false),true), StructField(v5cs,ArrayType(StructType(StructField(issuedDate,StringType,true)),false),true), StructField(plateChanges,ArrayType(StructType(StructField(currentRegistration,StringType,true), StructField(previousRegistration,StringType,true), StructField(startDate,StringType,true)),false),true), StructField(insuranceWriteoffs,ArrayType(StructType(StructField(type,StringType,true), StructField(lossDate,StringType,true), StructField(removedDate,StringType,true)),false),true), StructField(highRiskMarkers,ArrayType(StructType(StructField(startDate,StringType,true), StructField(type,StringType,true), StructField(extraInfo,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(reference,StringType,true)),false),true), StructField(keeperChanges,ArrayType(StructType(StructField(startDate,StringType,true)),false),true), StructField(colourChanges,ArrayType(StructType(StructField(startDate,StringType,true), StructField(previousColour,StringType,true)),false),true), StructField(odometerReadings,ArrayType(StructType(StructField(performed,StringType,true), StructField(source,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true)),false),true),StructField(previousSearches,ArrayType(StructType(StructField(performed,StringType,true), StructField(typeOfBusiness,StringType,true)),false),true)),true)),false)

      pending

      assert(schema.length == 1)
      println(schema)

      val actual = StructTypeToCaseClass(schema).buildCaseClassString

      val expected = "case class GeneratedClass(" +
        "aBoolean: Boolean, " +
        "anArray: Seq[], " +
        "aDate: java.sql.Date, " +
        "aString: String" +
        ")"

      assert(actual == expected)
    }

    it("generates case class for a schema with nested non-standard classes") {
      val simpleSchema = StructType(Seq(
        StructField("aString", StringType, nullable = true),
        StructField("anInteger", IntegerType, nullable = false))
      )
      val complexSchema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("testClass", simpleSchema, nullable = true),
        StructField("testClasses", ArrayType(simpleSchema))
      ))

      val actual = StructTypeToCaseClass(complexSchema).buildCaseClassString

      val expected =
              "case class GeneratedClass1(" +
              "aString: String, " +
              "anInteger: Int" +
              ")" + "\n" +
              "case class GeneratedClass2(" +
              "aString: String, " +
              "anInteger: Int" +
              ")" + "\n" +
              "case class GeneratedClass(" +
              "name: String, " +
              "testClass: GeneratedClass1, " +
              "testClasses: Seq[GeneratedClass2]" +
              ")"

      assert(actual == expected)
    }

    it("generates case class for funny field names") {
        pending
    }

    it("generates case class for nullable types") {
        pending
    }

    it("generates case class for ") {
      println("template")
      pending
      assert("handy line for a debug breakpoint".isInstanceOf[String])
    }

    it("generates case class for vehicle-check") {
      val schemaName = "schema.avsc"
      val vehicleCheckSchemaPath = this.getClass.getResource(schemaName)
      println(vehicleCheckSchemaPath)
      val stream = this.getClass.getResourceAsStream(schemaName)
      val parser: Parser = new Parser()
      val avroSchema = parser.parse(stream)
      println(s"$schemaName: $avroSchema")
      //vehicle-check.avsc: {"type":"record","name":"topLevelRecord","namespace":"topLevelRecord","fields":[{"name":"dataSource","type":["string","null"]},{"name":"errorMessage","type":["string","null"]},{"name":"eventSource","type":["string","null"]},{"name":"eventTime","type":["string","null"]},{"name":"key","type":["string","null"]},{"name":"request","type":[{"type":"record","name":"request","namespace":"topLevelRecord.request","fields":[{"name":"did","type":["string","null"]},{"name":"registration","type":["string","null"]},{"name":"sourceSystem","type":["string","null"]},{"name":"stockItemId","type":["string","null"]},{"name":"ursId","type":["string","null"]},{"name":"user","type":["string","null"]},{"name":"vin","type":["string","null"]}]},"null"]},{"name":"status","type":["string","null"]},{"name":"vehicleCheck","type":[{"type":"record","name":"vehicleCheck","namespace":"topLevelRecord.vehicleCheck","fields":[{"name":"co2Emissions","type":[{"type":"record","name":"co2Emissions","namespace":"topLevelRecord.vehicleCheck.co2Emissions","fields":[{"name":"unit","type":["string","null"]},{"name":"value","type":["double","null"]}]},"null"]},{"name":"colourChanged","type":["boolean","null"]},{"name":"colourChanges","type":[{"type":"array","items":["string","null"]},"null"]},{"name":"dvlaMake","type":["string","null"]},{"name":"dvlaModel","type":["string","null"]},{"name":"engineNumber","type":["string","null"]},{"name":"exported","type":["boolean","null"]},{"name":"exportedDate","type":["string","null"]},{"name":"financeAgreements","type":[{"type":"array","items":[{"type":"record","name":"financeAgreements","namespace":"topLevelRecord.vehicleCheck.financeAgreements","fields":[{"name":"agreementId","type":["string","null"]},{"name":"company","type":["string","null"]},{"name":"startDate","type":["string","null"]},{"name":"telephoneNumber","type":["string","null"]},{"name":"term","type":["long","null"]},{"name":"type","type":["string","null"]}]},"null"]},"null"]},{"name":"highRisk","type":["boolean","null"]},{"name":"highRiskMarkers","type":[{"type":"array","items":["string","null"]},"null"]},{"name":"id","type":["string","null"]},{"name":"imported","type":["boolean","null"]},{"name":"insuranceWriteoffType","type":["string","null"]},{"name":"insuranceWriteoffs","type":[{"type":"array","items":["string","null"]},"null"]},{"name":"keeperChanges","type":[{"type":"array","items":[{"type":"record","name":"keeperChanges","namespace":"topLevelRecord.vehicleCheck.keeperChanges","fields":[{"name":"startDate","type":["string","null"]}]},"null"]},"null"]},{"name":"mileageDiscrepancy","type":["boolean","null"]},{"name":"motTests","type":[{"type":"array","items":[{"type":"record","name":"motTests","namespace":"topLevelRecord.vehicleCheck.motTests","fields":[{"name":"expiryDate","type":["string","null"]},{"name":"motTestItems","type":[{"type":"array","items":[{"type":"record","name":"motTestItems","namespace":"topLevelRecord.vehicleCheck.motTests.motTestItems","fields":[{"name":"notes","type":["string","null"]},{"name":"type","type":["string","null"]}]},"null"]},"null"]},{"name":"motTestNumber","type":["string","null"]},{"name":"odometerReading","type":[{"type":"record","name":"odometerReading","namespace":"topLevelRecord.vehicleCheck.motTests.odometerReading","fields":[{"name":"unit","type":["string","null"]},{"name":"value","type":["double","null"]}]},"null"]},{"name":"performed","type":["string","null"]},{"name":"testResult","type":["string","null"]}]},"null"]},"null"]},{"name":"odometerReadings","type":[{"type":"array","items":["string","null"]},"null"]},{"name":"performed","type":["string","null"]},{"name":"plateChanges","type":[{"type":"array","items":["string","null"]},"null"]},{"name":"policeStolenMarker","type":[{"type":"record","name":"policeStolenMarker","namespace":"topLevelRecord.vehicleCheck.policeStolenMarker","fields":[{"name":"policeForce","type":["string","null"]},{"name":"recordedDate","type":["string","null"]},{"name":"telephoneNumber","type":["string","null"]}]},"null"]},{"name":"previousSearches","type":[{"type":"array","items":[{"type":"record","name":"previousSearches","namespace":"topLevelRecord.vehicleCheck.previousSearches","fields":[{"name":"performed","type":["string","null"]},{"name":"typeOfBusiness","type":["string","null"]}]},"null"]},"null"]},{"name":"privateFinance","type":["boolean","null"]},{"name":"registration","type":["string","null"]},{"name":"registrationChanged","type":["boolean","null"]},{"name":"registrationDate","type":["string","null"]},{"name":"scrapped","type":["boolean","null"]},{"name":"scrappedDate","type":["string","null"]},{"name":"stolen","type":["boolean","null"]},{"name":"tradeFinance","type":["boolean","null"]},{"name":"v5cs","type":[{"type":"array","items":[{"type":"record","name":"v5cs","namespace":"topLevelRecord.vehicleCheck.v5cs","fields":[{"name":"issuedDate","type":["string","null"]}]},"null"]},"null"]},{"name":"vin","type":["string","null"]},{"name":"yearOfManufacture","type":["long","null"]}]},"null"]}]}
      //schema.avsc: {"type":"record","name":"VehicleCheckEvent","namespace":"uk.co.autotrader.forge.model.vehiclecheck","fields":[{"name":"key","type":["null","string"]},{"name":"eventSource","type":["null","string"]},{"name":"eventTime","type":["null","string"]},{"name":"request","type":["null",{"type":"record","name":"Request","fields":[{"name":"vin","type":["null","string"]},{"name":"registration","type":["null","string"]},{"name":"did","type":["null","string"]},{"name":"ursId","type":["null","string"]},{"name":"user","type":["null","string"]},{"name":"sourceSystem","type":["null","string"]},{"name":"stockItemId","type":["null","string"]}]}]},{"name":"status","type":["null","string"]},{"name":"errorMessage","type":["null","string"]},{"name":"dataSource","type":["null","string"]},{"name":"vehicleCheck","type":["null",{"type":"record","name":"VehicleCheck","fields":[{"name":"id","type":["null","string"]},{"name":"performed","type":["null","string"]},{"name":"vin","type":["null","string"]},{"name":"registration","type":["null","string"]},{"name":"engineNumber","type":["null","string"]},{"name":"dvlaMake","type":["null","string"]},{"name":"dvlaModel","type":["null","string"]},{"name":"yearOfManufacture","type":["null",{"type":"int","java-class":"java.lang.Integer"}]},{"name":"registrationDate","type":["null","string"]},{"name":"insuranceWriteoffType","type":["null","string"]},{"name":"scrappedDate","type":["null","string"]},{"name":"exportedDate","type":["null","string"]},{"name":"co2Emissions","type":["null",{"type":"record","name":"EmissionMeasure","namespace":"uk.co.autotrader","fields":[{"name":"unit","type":["null",{"type":"enum","name":"Emission","namespace":"uk.co.autotrader.forge.model.common.fieldtypes.measures.units","symbols":["GRAMS_PER_KILOMETRE"]}]},{"name":"value","type":["null",{"type":"string","java-class":"java.math.BigDecimal"}]}]}]},{"name":"stolen","type":["null","boolean"]},{"name":"scrapped","type":["null","boolean"]},{"name":"exported","type":["null","boolean"]},{"name":"imported","type":["null","boolean"]},{"name":"highRisk","type":["null","boolean"]},{"name":"privateFinance","type":["null","boolean"]},{"name":"tradeFinance","type":["null","boolean"]},{"name":"mileageDiscrepancy","type":["null","boolean"]},{"name":"registrationChanged","type":["null","boolean"]},{"name":"colourChanged","type":["null","boolean"]},{"name":"policeStolenMarker","type":["null",{"type":"record","name":"PoliceStolenMarker","fields":[{"name":"recordedDate","type":["null","string"]},{"name":"policeForce","type":["null","string"]},{"name":"telephoneNumber","type":["null","string"]}]}]},{"name":"motTests","type":["null",{"type":"array","items":{"type":"record","name":"MotTest","fields":[{"name":"motTestNumber","type":["null","string"]},{"name":"performed","type":["null","string"]},{"name":"testResult","type":["null","string"]},{"name":"expiryDate","type":["null","string"]},{"name":"odometerReading","type":["null",{"type":"record","name":"LengthMeasure","namespace":"uk.co.autotrader","fields":[{"name":"unit","type":["null",{"type":"enum","name":"Length","namespace":"uk.co.autotrader.forge.model.common.fieldtypes.measures.units","symbols":["MILLIMETRE","CENTIMETRE","METRE","KILOMETRE","INCH","FOOT","YARD","MILE"]}]},{"name":"value","type":["null",{"type":"string","java-class":"java.math.BigDecimal"}]}]}]},{"name":"motTestItems","type":["null",{"type":"array","items":{"type":"record","name":"MotTestItem","fields":[{"name":"type","type":["null","string"]},{"name":"notes","type":["null","string"]}]}}]}]}}]},{"name":"financeAgreements","type":["null",{"type":"array","items":{"type":"record","name":"FinanceAgreement","fields":[{"name":"agreementId","type":["null","string"]},{"name":"company","type":["null","string"]},{"name":"telephoneNumber","type":["null","string"]},{"name":"startDate","type":["null","string"]},{"name":"term","type":["null",{"type":"int","java-class":"java.lang.Integer"}]},{"name":"type","type":["null","string"]}]}}]},{"name":"v5cs","type":["null",{"type":"array","items":{"type":"record","name":"V5C","fields":[{"name":"issuedDate","type":["null","string"]}]}}]},{"name":"plateChanges","type":["null",{"type":"array","items":{"type":"record","name":"PlateChange","fields":[{"name":"currentRegistration","type":["null","string"]},{"name":"previousRegistration","type":["null","string"]},{"name":"startDate","type":["null","string"]}]}}]},{"name":"insuranceWriteoffs","type":["null",{"type":"array","items":{"type":"record","name":"InsuranceWriteoff","fields":[{"name":"type","type":["null","string"]},{"name":"lossDate","type":["null","string"]},{"name":"removedDate","type":["null","string"]}]}}]},{"name":"highRiskMarkers","type":["null",{"type":"array","items":{"type":"record","name":"HighRiskMarker","fields":[{"name":"startDate","type":["null","string"]},{"name":"type","type":["null","string"]},{"name":"extraInfo","type":["null","string"]},{"name":"company","type":["null","string"]},{"name":"telephoneNumber","type":["null","string"]},{"name":"reference","type":["null","string"]}]}}]},{"name":"keeperChanges","type":["null",{"type":"array","items":{"type":"record","name":"KeeperChange","fields":[{"name":"startDate","type":["null","string"]}]}}]},{"name":"colourChanges","type":["null",{"type":"array","items":{"type":"record","name":"ColourChange","fields":[{"name":"startDate","type":["null","string"]},{"name":"previousColour","type":["null","string"]}]}}]},{"name":"odometerReadings","type":["null",{"type":"array","items":{"type":"record","name":"OdometerReading","fields":[{"name":"performed","type":["null","string"]},{"name":"source","type":["null","string"]},{"name":"odometerReading","type":["null","uk.co.autotrader.LengthMeasure"]}]}}]},{"name":"previousSearches","type":["null",{"type":"array","items":{"type":"record","name":"PreviousSearch","fields":[{"name":"performed","type":["null","string"]},{"name":"typeOfBusiness","type":["null","string"]}]}}]}]}]}]}

      val schema = SchemaConverters.toSqlType(avroSchema)
      println(s"$schemaName: $schema")
      //vehicle-check.avsc: SchemaType(StructType(StructField(dataSource,StringType,true), StructField(errorMessage,StringType,true), StructField(eventSource,StringType,true), StructField(eventTime,StringType,true), StructField(key,StringType,true), StructField(request,StructType(StructField(did,StringType,true), StructField(registration,StringType,true), StructField(sourceSystem,StringType,true), StructField(stockItemId,StringType,true), StructField(ursId,StringType,true), StructField(user,StringType,true), StructField(vin,StringType,true)),true), StructField(status,StringType,true), StructField(vehicleCheck,StructType(StructField(co2Emissions,StructType(StructField(unit,StringType,true), StructField(value,DoubleType,true)),true), StructField(colourChanged,BooleanType,true), StructField(colourChanges,ArrayType(StringType,true),true), StructField(dvlaMake,StringType,true), StructField(dvlaModel,StringType,true), StructField(engineNumber,StringType,true), StructField(exported,BooleanType,true), StructField(exportedDate,StringType,true), StructField(financeAgreements,ArrayType(StructType(StructField(agreementId,StringType,true), StructField(company,StringType,true), StructField(startDate,StringType,true), StructField(telephoneNumber,StringType,true), StructField(term,LongType,true), StructField(type,StringType,true)),true),true), StructField(highRisk,BooleanType,true), StructField(highRiskMarkers,ArrayType(StringType,true),true), StructField(id,StringType,true), StructField(imported,BooleanType,true), StructField(insuranceWriteoffType,StringType,true), StructField(insuranceWriteoffs,ArrayType(StringType,true),true), StructField(keeperChanges,ArrayType(StructType(StructField(startDate,StringType,true)),true),true), StructField(mileageDiscrepancy,BooleanType,true), StructField(motTests,ArrayType(StructType(StructField(expiryDate,StringType,true), StructField(motTestItems,ArrayType(StructType(StructField(notes,StringType,true), StructField(type,StringType,true)),true),true), StructField(motTestNumber,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,DoubleType,true)),true), StructField(performed,StringType,true), StructField(testResult,StringType,true)),true),true), StructField(odometerReadings,ArrayType(StringType,true),true), StructField(performed,StringType,true), StructField(plateChanges,ArrayType(StringType,true),true), StructField(policeStolenMarker,StructType(StructField(policeForce,StringType,true), StructField(recordedDate,StringType,true), StructField(telephoneNumber,StringType,true)),true), StructField(previousSearches,ArrayType(StructType(StructField(performed,StringType,true), StructField(typeOfBusiness,StringType,true)),true),true), StructField(privateFinance,BooleanType,true), StructField(registration,StringType,true), StructField(registrationChanged,BooleanType,true), StructField(registrationDate,StringType,true), StructField(scrapped,BooleanType,true), StructField(scrappedDate,StringType,true), StructField(stolen,BooleanType,true), StructField(tradeFinance,BooleanType,true), StructField(v5cs,ArrayType(StructType(StructField(issuedDate,StringType,true)),true),true), StructField(vin,StringType,true), StructField(yearOfManufacture,LongType,true)),true)),false)
      //schema.avsc: SchemaType(StructType(StructField(key,StringType,true), StructField(eventSource,StringType,true), StructField(eventTime,StringType,true), StructField(request,StructType(StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(did,StringType,true), StructField(ursId,StringType,true), StructField(user,StringType,true), StructField(sourceSystem,StringType,true), StructField(stockItemId,StringType,true)),true), StructField(status,StringType,true), StructField(errorMessage,StringType,true), StructField(dataSource,StringType,true), StructField(vehicleCheck,StructType(StructField(id,StringType,true), StructField(performed,StringType,true), StructField(vin,StringType,true), StructField(registration,StringType,true), StructField(engineNumber,StringType,true), StructField(dvlaMake,StringType,true), StructField(dvlaModel,StringType,true), StructField(yearOfManufacture,IntegerType,true), StructField(registrationDate,StringType,true), StructField(insuranceWriteoffType,StringType,true), StructField(scrappedDate,StringType,true), StructField(exportedDate,StringType,true), StructField(co2Emissions,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(stolen,BooleanType,true), StructField(scrapped,BooleanType,true), StructField(exported,BooleanType,true), StructField(imported,BooleanType,true), StructField(highRisk,BooleanType,true), StructField(privateFinance,BooleanType,true), StructField(tradeFinance,BooleanType,true), StructField(mileageDiscrepancy,BooleanType,true), StructField(registrationChanged,BooleanType,true), StructField(colourChanged,BooleanType,true), StructField(policeStolenMarker,StructType(StructField(recordedDate,StringType,true), StructField(policeForce,StringType,true), StructField(telephoneNumber,StringType,true)),true), StructField(motTests,ArrayType(StructType(StructField(motTestNumber,StringType,true), StructField(performed,StringType,true), StructField(testResult,StringType,true), StructField(expiryDate,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true), StructField(motTestItems,ArrayType(StructType(StructField(type,StringType,true), StructField(notes,StringType,true)),false),true)),false),true), StructField(financeAgreements,ArrayType(StructType(StructField(agreementId,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(startDate,StringType,true), StructField(term,IntegerType,true), StructField(type,StringType,true)),false),true), StructField(v5cs,ArrayType(StructType(StructField(issuedDate,StringType,true)),false),true), StructField(plateChanges,ArrayType(StructType(StructField(currentRegistration,StringType,true), StructField(previousRegistration,StringType,true), StructField(startDate,StringType,true)),false),true), StructField(insuranceWriteoffs,ArrayType(StructType(StructField(type,StringType,true), StructField(lossDate,StringType,true), StructField(removedDate,StringType,true)),false),true), StructField(highRiskMarkers,ArrayType(StructType(StructField(startDate,StringType,true), StructField(type,StringType,true), StructField(extraInfo,StringType,true), StructField(company,StringType,true), StructField(telephoneNumber,StringType,true), StructField(reference,StringType,true)),false),true), StructField(keeperChanges,ArrayType(StructType(StructField(startDate,StringType,true)),false),true), StructField(colourChanges,ArrayType(StructType(StructField(startDate,StringType,true), StructField(previousColour,StringType,true)),false),true), StructField(odometerReadings,ArrayType(StructType(StructField(performed,StringType,true), StructField(source,StringType,true), StructField(odometerReading,StructType(StructField(unit,StringType,true), StructField(value,StringType,true)),true)),false),true), StructField(previousSearches,ArrayType(StructType(StructField(performed,StringType,true), StructField(typeOfBusiness,StringType,true)),false),true)),true)),false)

      assert(!schema.nullable)

      assert(schema.dataType.isInstanceOf[StructType])
      assert(schema.dataType.typeName == "struct")

      //      schema.dataType.

      pending
      assert("handy line for a debug breakpoint".isInstanceOf[String])
    }
  }
}
