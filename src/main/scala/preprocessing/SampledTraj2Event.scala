package preprocessing

import instances.{Duration, Point, Trajectory}
import operatorsNew.converter.Traj2EventConverter
import org.apache.spark.sql.SparkSession
import preprocessing.geoJson2ParquetTraj.T
import utils.Config

object SampledTraj2Event {
  case class E(lon: Double, lat:Double, t: Double, id: String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SampledTraj2Event")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val fileName = args(0)
    import spark.implicits._

    val readDs = spark.read.parquet(fileName)
    val trajectoryRDD = readDs.as[T].rdd.map(x => {
      val pointArr = x.entries.map(e => Point(e.lon, e.lat)).toArray
      val durationArr = x.entries.map(x => Duration(x.t)).toArray
      val valueArr = x.entries.map(_ => None).toArray
      Trajectory(pointArr, durationArr, valueArr, x.id)
    })

    val converter = new Traj2EventConverter[None.type, String]
    val pointRDD = converter.convert(trajectoryRDD)

    val pointDS = pointRDD.map(p =>
      E(p.entries.head.spatial.getX, p.entries.head.spatial.getX, p.entries.head.temporal.start, p.data)).toDS()
    pointDS.printSchema()
    pointDS.show(5)

    pointDS.write.parquet(fileName.replace("taxi","taxi_point"))
  }
}
