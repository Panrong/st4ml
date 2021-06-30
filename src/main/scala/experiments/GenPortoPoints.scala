package experiments

import operators.convertion.Traj2PointConverter
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajFile
import utils.Config

object GenPortoPoints {
  case class Document(id: String, latitude: Double, longitude: Double, timestamp: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GenPortoPoints")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajectoryFile = Config.get("portoData")
    val trajRDD = ReadTrajFile(trajectoryFile, 16)
    val converter = new Traj2PointConverter()
    val pointRDD = converter.convert(trajRDD).map(point => Document(point.id, point.lat, point.lon, point.timeStamp._1 ))
    import spark.implicits._
    val pointDs = pointRDD.toDS()
    pointDs.printSchema()
    println(pointDs.count)
    pointDs.write.parquet(args(0))
  }
}
