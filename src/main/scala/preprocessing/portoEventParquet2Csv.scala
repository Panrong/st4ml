package preprocessing

import instances.{Duration, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import utils.Config
import utils.TimeParsing.timeLong2String

case class P(id: String, lon: Double, lat: Double, t: Long)

object portoEventParquet2Csv {
  case class geometry(coordinates: Seq[Seq[Double]], `type`: String = "LineString")

  case class properties(TIMESTAMP: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("portoParquet2Geojson")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val res = args(1)
    val readDs = spark.read.parquet(fileName)
    readDs.withColumnRenamed("lon", "longitude")
      .withColumnRenamed("lat", "latitude")
      .withColumnRenamed("t", "timestamp")
      .select("id", "latitude", "longitude", "timestamp")
      .repartition(256).write.csv(res)

    sc.stop
  }
}
