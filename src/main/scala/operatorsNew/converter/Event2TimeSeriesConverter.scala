package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, RTree, TimeSeries}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag

// map each partition to a time series
class Event2TimeSeriesConverter[S <: Geometry, V, D, VTS, DTS](f: Array[Event[S, V, D]] => VTS,
                                                               tArray: Array[Duration],
                                                               d: DTS = None) extends Converter {
  type I = Event[S, V, D]
  type O = TimeSeries[VTS, DTS]

  val tMap: Array[(Int, Duration)] = tArray.zipWithIndex.map(_.swap)
  var rTree: Option[RTree[Polygon]] = None

  def buildRTree(temporals: Array[Duration]): RTree[Polygon] = {
    val r = math.sqrt(temporals.length).toInt
    val entries = temporals.map(t => Extent(t.start, 0, t.end, 1).toPolygon)
      .zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[Polygon](entries, r)
  }
  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val events = partition.toArray
      val emptyTs = TimeSeries.empty[I](tArray)
      Iterator(emptyTs.attachInstance(events)
        .mapValue(f)
        .mapData(_ => d))
    })
  }
  def convertWithRTree(input: RDD[I]): RDD[O] = {
    rTree = Some(buildRTree(tMap.map(_._2)))
    val spark = SparkSession.builder().getOrCreate()
    val rTreeBc = spark.sparkContext.broadcast(rTree)
    input.mapPartitions(partition => {
      val events = partition.toArray
      val emptyTs = TimeSeries.empty[I](tArray)
      emptyTs.rTree = rTreeBc.value
      Iterator(emptyTs.attachInstanceRTree(events)
        .mapValue(f)
        .mapData(_ => d))
    })
  }
}

object Event2TimeSeriesConverterTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val events = Array(
      Event(Point(1, 2), Duration(34), d = "0"),
      Event(Point(3, 4), Duration(94), d = "1"),
      Event(Point(5, 6), Duration(100), d = "2"),
      Event(Point(7, 8), Duration(174), d = "3"),
      Event(Point(9, 10), Duration(234), d = "4"),
      Event(Point(11, 12), Duration(284), d = "5"),
      Event(Point(13, 14), Duration(334), d = "6"),
      Event(Point(15, 16), Duration(364), d = "7")
    )

    val eventRDD = sc.parallelize(events)

    val tArray = Array(
      Duration(0, 100),
      Duration(100, 200),
      Duration(200, 300),
      Duration(300, 400)
    )

    val f: Array[Event[Point, None.type, String]] => Int = _.length

    //    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val countConverter = new Event2TimeSeriesConverter(f, tArray)

    val tsRDD = countConverter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}