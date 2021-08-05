package preprocessing

import instances.{Duration, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import utils.Config

object geoJson2ParquetTraj {
  case class T(id: String, entries: Seq[E])

  case class E(lon: Double, lat: Double, t: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("geoJson2ParquetTraj")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val outputName = args(1)

    val trajRDD = ReadTrajJsonFile(fileName)
    println(trajRDD.count)
    import spark.implicits._
    val trajDS = trajRDD.map(traj => {
      val entries = traj.points.map(point => E(point.lon, point.lat, point.timeStamp._1)).toSeq
      T(traj.id, entries)
    }).toDS
    trajDS.show(5)
    trajDS.write.parquet(outputName)

    // read parquet
    val readDs = spark.read.parquet(args(1))
    readDs.printSchema()
    readDs.show(5)

    val trajectoryRDD = readDs.as[T].rdd.map(x => {
      val pointArr = x.entries.map(e => Point(e.lon, e.lat)).toArray
      val durationArr = x.entries.map(x => Duration(x.t)).toArray
      val valueArr = x.entries.map(_ => None).toArray
      Trajectory(pointArr, durationArr, valueArr, x.id)
    })
    trajectoryRDD.take(5).foreach(println)

    sc.stop()
  }
}
