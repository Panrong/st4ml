package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, TimeSeries}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag

// map each partition to a time series
class Event2TimeSeriesConverter[S <: Geometry, V, D, VTS, DTS](f: Array[Event[S, V, D]] => VTS,
                                                               tArray: Array[Duration],
                                                               d: DTS = None,
                                                               instant: Boolean = true) extends Converter {
  type I = Event[S, V, D]
  type O = TimeSeries[VTS, DTS]

  val tMap: Array[(Int, Duration)] = tArray.zipWithIndex.map(_.swap)

  //  // check if tArray is standard
  //  var interval: Long = 0
  //
  //  def checkStandard(): Boolean = {
  //    interval = tArray.head.seconds
  //    for (Array(x1, x2) <- tArray.sliding(2)) {
  //      if (x1.end != x2.start) {
  //        println("The temporal ranges are not continuous. The calculation may be slow.")
  //        return false
  //      }
  //      if (x2.seconds != interval) {
  //        println("The temporal ranges are not of equal length. The calculation may be slow.")
  //        return false
  //      }
  //    }
  //    true
  //  }
  //
  //  val standard: Boolean = checkStandard()

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val eventSlots = partition.map(event => {
        val slots = tMap.filter(_._2.intersects(event.duration))
        if (instant) (event, Array(slots.head._1))
        else (event, slots.map(_._1))
      }).flatMap {
        case (event, slots) => slots.map(slot => (slot, event)).toIterator
      }.toArray.groupBy(_._1).mapValues(x => x.map(_._2)).toArray

      val entries = eventSlots.map(slot => {
        val spatial = mbrAll(slot._2)
        val temporal = tMap(slot._1)._2
        val v = f(slot._2)
        new Entry(spatial, temporal, v)
      })
      val ts = new TimeSeries[VTS, DTS](entries, data = d)
      Iterator(ts)
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