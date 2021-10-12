package experiments

import org.apache.spark.sql.SparkSession
import instances.{Duration, Event, Point, Trajectory}
import utils.Config
import operatorsNew.selector.SelectionUtils._
object ReformatData extends App {
  /** reformat the original porto data to STT format */
  case class E(id: String, lon: Double, lat: Double, t: Long) // event
  case class E2(lon: Double, lat: Double, t: Long)
  case class T(id: String, entries: Array[E2]) // trajectory

  val fileName = args(0)
  val numPartitions = args(1).toInt
  val resDir = args(2)
  val t = args(3)

  val spark = SparkSession.builder()
    .appName("ReformatData")
    .master(Config.get("master"))
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val readDs = spark.read.parquet(fileName)
  import spark.implicits._
  if(t == "traj") {
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
    println(trajRDD.take(5).deep)
    trajRDD.toDs().repartition(numPartitions).write.parquet(resDir)
  }
  else if (t == "event"){
    val eventRDD = readDs.as[E].rdd.map(x => {
      Event(Point(x.lon, x.lat), Duration(x.t), d = x.id)
    })
    println(eventRDD.take(5).deep)
    eventRDD.toDs().repartition(numPartitions).write.parquet(resDir)
  }

  sc.stop()
}
