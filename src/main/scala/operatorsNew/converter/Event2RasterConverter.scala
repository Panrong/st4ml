package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, Raster}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Event2RasterConverter[S <: Geometry, V, D, VR, DR](f: Array[Event[S, V, D]] => VR,
                                                         polygonArr: Array[Polygon],
                                                         durArr: Array[Duration],
                                                         d: DR = None) extends Converter {
  type I = Event[S, V, D]
  type O = Raster[Polygon, VR, DR]

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      Iterator(emptyRaster.attachInstance(trajs)
        .mapValue(f)
        .mapData(_ => d))
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

    val stArray = Array(
      (Extent(0, 0, 5, 5).toPolygon, Duration(0, 100)), // 2
      (Extent(2, 2, 8, 8).toPolygon, Duration(100, 200)), // 2
      (Extent(5, 5, 10, 10).toPolygon, Duration(200, 300)), // 1
      (Extent(10, 10, 20, 20).toPolygon, Duration(300, 400)) // 2
    )

    val f: Array[Event[Point, None.type, String]] => Int = _.length

    //    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val converter = new Event2RasterConverter(f, stArray.map(_._1), stArray.map(_._2))

    val tsRDD = converter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}
