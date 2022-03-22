package st4ml.operators.converter

import st4ml.instances.GeometryImplicits._
import st4ml.instances._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

import scala.reflect.ClassTag

class Event2EventConverter[V: ClassTag, D: ClassTag](map: Array[LineString],
                                                     dist: Double = 500) extends Converter {
  type I = Event[Point, V, D]
  type O = Event[Point, V, String]
  override val optimization: String = ""
  val entries: Array[(LineString, String, Int)] = map.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
  val rTree: RTree[LineString, String] = RTree(entries, scala.math.sqrt(entries.length).toInt)

  def convert(input: RDD[I], discard: Boolean = false): RDD[O] = {
    val resRDD = input.map(event => {
      val candidates = rTree.distanceRange(event.toGeometry, dist)
      val id = if (!candidates.isEmpty) candidates.map(x => (x._1.distance(event.toGeometry), x._2.toInt))
        .minBy(_._1)._2
      else -1
      if (id == -1) Event(event.entries, "not-matched")
      else {
        val projected = event.spatialCenter.project(map(id))._1
        Event(projected, event.entries.head.temporal, event.entries.head.value, event.data.toString)
      }
    })
    if (discard) resRDD.filter(_.data != "not-matched") else resRDD
  }
}

object E2ETest extends App {
  val events = Array(
    Event(Point(0, 0), Duration(0)),
    Event(Point(1.2, 1.4), Duration(0)),
    Event(Point(0.5, 0.7), Duration(0)),
    Event(Point(2.2, 1.8), Duration(0)),
  )
  val spark = SparkSession.builder()
    .appName("addNoise")
    .master(Config.get("master"))
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val eventsRDD = sc.parallelize(events)
  val map = Array(
    LineString(Point(0, 0), Point(2, 2)),
    LineString(Point(2, 2), Point(4, 3)),
    LineString(Point(10, 0), Point(2, 2)),
    LineString(Point(22, 2), Point(4, 3)),
    LineString(Point(20, 0), Point(2, 2)),
    LineString(Point(32, 2), Point(4, 3)),
    LineString(Point(40, 0), Point(2, 2)),
    LineString(Point(42, 2), Point(4, 3)),
    LineString(Point(50, 0), Point(2, 2)),
    LineString(Point(62, 2), Point(4, 3)),
  )
  val converter = new Event2EventConverter[None.type, None.type](map, 1e10)
  val cRDD = converter.convert(eventsRDD)
  println(cRDD.collect.deep)
}