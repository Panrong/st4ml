package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, Raster}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Event2RasterConverter[S <: Geometry, V, D, VR, DR](f: Array[Event[S, V, D]] => VR,
                                                         stArray: Array[(Polygon, Duration)],
                                                         d: DR = None,
                                                         instant: Boolean = true) extends Converter {
  type I = Event[S, V, D]
  type O = Raster[Polygon, VR, DR]

  val stMap: Array[(Int, (Polygon, Duration))] = stArray.zipWithIndex.map(_.swap)

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val eventSlots = partition.map(event => {
        val slots = stMap.filter {
          case (_, (s, t)) => t.intersects(event.duration) && s.intersects(event.extent)
        }
        if (instant) (event, Array(slots.map(_._1).head))
        else (event, slots.map(_._1))
      }).flatMap {
        case (event, slots) => slots.map(slot => (slot, event)).toIterator
      }.toArray.groupBy(_._1).mapValues(x => x.map(_._2)).toArray

      val entries = eventSlots.map(slot => {
        val spatial = stMap(slot._1)._2._1
        val temporal = stMap(slot._1)._2._2
        val v = f(slot._2)
        new Entry(spatial, temporal, v)
      })
      val raster = new Raster[Polygon, VR, DR](entries, data = d)
      Iterator(raster)
    })
  }
}

object Event2RasterConverterTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val events = Array(
      Event(Point(1, 2), Duration(34), d = "0"),
      Event(Point(3, 4), Duration(94), d = "1"),
      Event(Point(5, 5), Duration(134), d = "2"),
      Event(Point(7, 8), Duration(174), d = "3"),
      Event(Point(9, 10), Duration(234), d = "4"),
      Event(Point(11, 12), Duration(284), d = "5"),
      Event(Point(13, 14), Duration(334), d = "6"),
      Event(Point(15, 16), Duration(364), d = "7")
    )

    val eventRDD = sc.parallelize(events)

    val sArray = Array(
      (Extent(0, 0, 5, 5).toPolygon, Duration(0, 100)), // 2
      (Extent(2, 2, 8, 8).toPolygon, Duration(100, 200)), // 2
      (Extent(5, 5, 10, 10).toPolygon, Duration(200, 300)), // 1
      (Extent(10, 10, 20, 20).toPolygon, Duration(300, 400)) // 2
    )

    val f: Array[Event[Point, None.type, String]] => Int = _.length

    //    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val countConverter = new Event2RasterConverter(f, sArray)

    val tsRDD = countConverter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}
