package examples

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.elasticsearch.spark.sql.sparkDatasetFunctions
import utils.Config

object VhcDatasetToEsDemo {

  case class CsvRecord(vehicleId: String,
                       timestamp1: Long,
                       timestamp2: Long,
                       vehicleType: String,
                       cameraId: String,
                       attribute1: Int,
                       attribute2: Int,
                       attribute3: Int)

  def parseTime(s: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(s).getTime / 1000
  }

  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("io-es-writing-vehicle")
      .master(Config.get("master"))
      .config("es.nodes", args(1))
      .config("es.port", args(2))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filePath = args(0)
    val esIndex = args(3)

    val schema = StructType(Array(
      StructField("vehicleId",StringType,true),
      StructField("timestamp1",StringType,true),
      StructField("timestamp2",StringType,true),
      StructField("vehicleType", StringType, true),
      StructField("cameraId", StringType, true),
      StructField("attribute1", IntegerType, true),
      StructField("attribute2", IntegerType, true),
      StructField("attribute3", IntegerType, true)
    ))

    import spark.implicits._

    val timeParserUDF = udf(parseTime _)
    val dataDf = spark.read.format("csv")
      .option("header", "false")
      .schema(schema)
      .load(filePath)
      .withColumn("timestamp1", timeParserUDF(col("timestamp1")))
      .withColumn("timestamp2", timeParserUDF(col("timestamp2")))

    val dataDs = dataDf.as[CsvRecord]
    dataDs.saveToEs(esIndex)

  }

}
