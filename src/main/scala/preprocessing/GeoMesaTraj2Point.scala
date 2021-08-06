package preprocessing

import org.apache.spark.sql.SparkSession
import utils.Config

object GeoMesaTraj2Point {
  case class P(id: String, lon: Double, lat: Double, t: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReadSyntheticJsonTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajRDD = ReadTrajJsonFile(args(0))
    val pointRDD = trajRDD.flatMap(traj => {
      val points = traj.points
      points.map(x => x.setID(traj.id))
    })
    import spark.implicits._
    val pointDs = pointRDD.map(x => P(id = x.id, lon = x.lon, lat = x.lat, t = x.timeStamp._1)).toDS()
    pointDs.write.parquet(args(1))

  }
}
