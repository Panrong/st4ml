package experiments

import st4ml.instances.{Duration, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

object ReformatHzTraj {
  case class E2(timestamp: String, latitude: String, longitude: String)

  case class T(id: String, points: Array[E2])

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

    val readDs = spark.read.json(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd
      .map(x => {
        try {
          val entries = x.points.map(p => (Point(p.longitude.toDouble, p.latitude.toDouble), Duration(p.timestamp.toLong), None))
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
