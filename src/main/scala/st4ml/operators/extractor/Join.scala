package st4ml.operators.extractor

import st4ml.instances.{Duration, Event, Geometry, Instance, Point, Polygon, RTree, Trajectory}
import st4ml.operators.selector.SelectionUtils.{E, T}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

import scala.reflect.ClassTag

object Join {
  private def buildRTree[S <: Geometry : ClassTag](geomArr: Array[S], durArr: Array[Duration]): RTree[S, String] = {
    val r = math.sqrt(geomArr.length).toInt
    var entries = new Array[(S, String, Int)](0)
    for (i <- geomArr.indices) {
      geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (geomArr(i).copy.asInstanceOf[S], (geomArr(i)).hashCode().toString, i)
    }
    RTree[S, String](entries, r, dimension = 3)
  }

  implicit class BcJoin[T1 <: Instance[_, _, _] : ClassTag](rdd: RDD[T1]) extends Serializable {
    def bcJoin[T2 <: Instance[_, _, _] : ClassTag](bcRdd: RDD[T2]): RDD[(T1, T2)] = {
      val spark = SparkSession.builder().getOrCreate()
      val bc = spark.sparkContext.broadcast(bcRdd.collect)
      rdd.flatMap {
        x => bc.value.filter(_.intersects(x.toGeometry, x.duration)).map(y => (x, y))
      }
    }

    def bcJoinRTree[T2 <: Instance[_, _, _] : ClassTag](bcRdd: RDD[T2]): RDD[(T2, Geometry)] = {
      val spark = SparkSession.builder().getOrCreate()
      val bc = spark.sparkContext.broadcast(bcRdd.collect)
      rdd.mapPartitions(p => {
        val instances = p.toArray
        val rTree = buildRTree(instances.map(_.toGeometry), instances.map(_.duration))
       println(instances.deep)
       println(bc.value.deep)
       println(bc.value.map(x => rTree.range3d(x)).deep)
        bc.value.flatMap(x => rTree.range3d(x).map(y => (x, y._1))).toIterator
      })
    }
  }
}

object JoinTest {
  //  case class Point(lon: Double, lat: Double, t: Long, value: String) {
  //    def tInside(y: (Long, Long)): Boolean = y._1 <= t && y._2 >= t
  //  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("test")
      .master(Config.get("master"))
      .getOrCreate()
    import spark.implicits._
    import st4ml.operators.extractor.Join._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
//    val eventFile = args(0)
//    val trajFile = args(1)
//    val eventRDD = spark.read.parquet(eventFile).drop("pId").as[E]
//      .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
//
//    val trajRDD = spark.read.parquet(trajFile).drop("pId").as[T]
//      .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])

   val trajRDD = sc.parallelize(Array(
    Event(Point(0,0), Duration(0)),
    Event(Point(0,1), Duration(1)),
    Event(Point(1,0), Duration(1)),
    Event(Point(1,1), Duration(0)),
    Event(Point(0,0), Duration(0)),
    Event(Point(0,1), Duration(1)),
    Event(Point(1,0), Duration(1)),
    Event(Point(1,1), Duration(0)),
   ), 1)

   val eventRDD = sc.parallelize(Array(
    Event(Point(0,0), Duration(0)),
    Event(Point(0,1), Duration(1)),
    Event(Point(1,0), Duration(0)),
    Event(Point(0,0), Duration(0)),
    Event(Point(0,1), Duration(1)),
    Event(Point(1,0), Duration(0)),
   ), 1)

    val joinRDD = eventRDD.bcJoinRTree(trajRDD).distinct
    joinRDD.collect.foreach(x => println(x._1, x._2))
    println(joinRDD.count)
  }
}