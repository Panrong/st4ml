// For enlarge the air quality dataset

package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import st4ml.instances.{Duration, Event, Point}
import st4ml.utils.Config
import org.apache.spark.sql.Row
import scala.util.Random

object EnlargeAir {
  case class AirRaw(
                     station_id: Int,
                     PM25_Concentration: Double,
                     PM10_Concentration: Double,
                     NO2_Concentration: Double,
                     CO_Concentration: Double,
                     O3_Concentration: Double,
                     SO2_Concentration: Double,
                     t: Long,
                     name_chinese: String,
                     name_english: String,
                     longitude: Double,
                     latitude: Double,
                     district_id: String)

  def main(args: Array[String]): Unit = {
    val stationSchema = StructType(Array(
      StructField("station_id", IntegerType, false),
      StructField("name_chinese", StringType, false),
      StructField("name_english", StringType, false),
      StructField("latitude", DoubleType, false),
      StructField("longitude", DoubleType, false),
      StructField("district_id", StringType, false)
    ))

    val dataDir = "/datasets/"

    val expansionStation = 19
    val expansionRecord = (6000, 5) // incremental of 10min, add 5 replication for each record
    val sigma = 500.0 / 111000
    val spark = SparkSession.builder()
      .appName("AirQuality")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val stationDf = spark.read.option("header", true).schema(stationSchema).csv(dataDir + "station.csv")

    val r1 = Random
    val r2 = Random
    val expandedStationRDD = stationDf
      .rdd.flatMap { x =>
      val lon = x.getDouble(4)
      val lat = x.getDouble(3)
      var newCoords = Array((lon, lat))
      for (_ <- Range(0, expansionStation)) {
        newCoords = newCoords :+ ((lon + r1.nextGaussian() * sigma, lat + r2.nextGaussian() * sigma))
      }
      newCoords.map(c => Row(x.getInt(0), x.getString(1), x.getString(2), c._2, c._1, x.getString(5)))
    }

    val expandedStationDf = spark.createDataFrame(expandedStationRDD, stationSchema)

    println(s"After expansionStation: ${expandedStationDf.count()} stations")

    val airSchema = StructType(Array(
      StructField("station_id", IntegerType, false),
      StructField("time", StringType, false),
      StructField("PM25_Concentration", DoubleType, true),
      StructField("PM10_Concentration", DoubleType, true),
      StructField("NO2_Concentration", DoubleType, true),
      StructField("CO_Concentration", DoubleType, true),
      StructField("O3_Concentration", DoubleType, true),
      StructField("SO2_Concentration", DoubleType, true)
    ))
    val aqDf = spark.read.option("header", true).schema(airSchema).csv(dataDir + "airquality.csv")
      .withColumn("t", unix_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
      .drop("time")
      .filter(col("PM25_Concentration").isNotNull && col("PM10_Concentration").isNotNull
        && col("NO2_Concentration").isNotNull && col("CO_Concentration").isNotNull
        && col("O3_Concentration").isNotNull && col("SO2_Concentration").isNotNull)
      .join(expandedStationDf, "station_id")

    aqDf.show(2)
    import spark.implicits._
    val aqRDD = aqDf.as[AirRaw].rdd
    val enlargedAqRDD = aqRDD.flatMap { x =>
      val tInterval = expansionRecord._1
      val num = expansionRecord._2
      val ts = Range(0, num + 1).map(t => t * tInterval + x.t)
      // we use the same air quality since it doesn't affect the computation time
      ts.map(t => AirRaw(x.station_id, x.PM25_Concentration, x.PM10_Concentration, x.NO2_Concentration,
        x.CO_Concentration, x.O3_Concentration, x.SO2_Concentration, t, x.name_chinese,
        x.name_english, x.latitude, x.longitude, x.district_id))
    }
    println(s"After expansionStation: ${enlargedAqRDD.count} records")
    enlargedAqRDD.toDS().write.parquet("/datasets/aq")
    // test
    val eventRDD = enlargedAqRDD.map(x => Event(Point(x.longitude, x.latitude), Duration(x.t), Array(x.PM25_Concentration, x.PM10_Concentration, x.NO2_Concentration,
      x.CO_Concentration, x.O3_Concentration, x.SO2_Concentration), x.station_id))
    eventRDD.take(2).foreach(println)
    sc.stop()
  }
}
