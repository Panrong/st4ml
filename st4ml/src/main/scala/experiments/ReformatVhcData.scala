package experiments

import st4ml.instances.{Duration, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

object ReformatVhcData {
  case class E(longitude: Double, latitude: Double, timestamp: Long)

  case class T(id: String, points: Array[E])

  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val numPartitions = args(1).toInt
    val resDir = args(2)

    val spark = SparkSession.builder()
      .appName("ReformatData")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd
      .filter(_.points.length > 2)
      .map(x => {
        try {
          val entries = x.points.map(p => (Point(p.longitude, p.latitude), Duration(p.timestamp), None))
          Some(Trajectory(entries, x.id))
        }
        catch {
          case _: Throwable => None
        }
      })
      .filter(_.isDefined).map(_.get)
    println(trajRDD.take(5).deep)
    import st4ml.operators.selector.SelectionUtils._
    trajRDD.toDs().repartition(numPartitions).write.parquet(resDir)

    sc.stop()
  }
}
