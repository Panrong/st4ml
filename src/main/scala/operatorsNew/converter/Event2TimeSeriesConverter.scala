package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Instance, Point, Polygon, RTree, TimeSeries}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// map each partition to a time series
class Event2TimeSeriesConverter[S <: Geometry, V, D, VTS, DTS](f: Array[Event[S, V, D]] => VTS,
                                                               tArray: Array[Duration],
                                                               d: DTS = None) extends Converter {
  type I = Event[S, V, D]
  type O = TimeSeries[VTS, DTS]

  val tMap: Array[(Int, Duration)] = tArray.sortBy(_.start).zipWithIndex.map(_.swap)
  var rTree: Option[RTree[Polygon]] = None

  def buildRTree(temporals: Array[Duration]): RTree[Polygon] = {
    val r = math.sqrt(temporals.length).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- temporals.zipWithIndex) {
      val p = Extent(0, 0, 1, 1).toPolygon
      p.setUserData(Array(i._1.start.toDouble, i._1.end.toDouble))
      entries = entries :+ (p.copy.asInstanceOf[Polygon], i._2.toString, i._2)
    }
    RTree[Polygon](entries, r, dimension = 3)
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

  def convertRegular(input: RDD[I]): RDD[O] = {
    val emptyTs = TimeSeries.empty[I](tArray)
    val tsMin = tMap.head._2.start
    val tsLength = tMap.head._2.seconds
    // val tsMax = tMap.last._2.end
    val tsSlots = tMap.length
    //assert(emptySm.isRegular, "The structure is not regular.")
    input.flatMap(e => {
      val tMin = e.duration.start
      val tMax = e.duration.end
      val idRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
      idRanges.map(x => (e, x))
    })
      .mapPartitions(partition => {
        val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
        val emptySm = TimeSeries.empty[I](tArray)
        Iterator(emptySm.createTimeSeries(events, getPolygonFromInstanceArray)
          .mapValue(f)
          .mapData(_ => d))
      })
  }

  def getPolygonFromInstanceArray[Event[S, V, D]](instanceArr: Array[I]): Polygon = {
    if (instanceArr.nonEmpty) {
      Extent(instanceArr.map(_.extent)).toPolygon
    }
    else Polygon.empty
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