package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, SpatialMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Event2SpatialMapConverter[S <: Geometry, V, D, VSM, DSM](f: Array[Event[S, V, D]] => VSM,
                                                               sArray: Array[Polygon],
                                                               d: DSM = None) extends Converter {
  type I = Event[S, V, D]
  type O = SpatialMap[VSM, DSM]

  val sMap: Array[(Int, Polygon)] = sArray.zipWithIndex.map(_.swap)

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val eventRegions = partition.map(event => {
        val regions = sMap.filter(_._2.intersects(event.extent))
        (event, regions.map(_._1))
      }).flatMap {
        case (event, slots) => slots.map(slot => (slot, event)).toIterator
      }.toArray.groupBy(_._1).mapValues(x => x.map(_._2)).toArray
      val entries = eventRegions.map(region => {
        val spatial = sMap(region._1)._2
        val temporal = durationAll(region._2)
        val v = f(region._2)
        new Entry(spatial, temporal, v)
      })
      val sm = new SpatialMap[VSM, DSM](entries, data = d)
      Iterator(sm)
    })
  }
}

object Event2SpatialMapConverterTest {
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
      Extent(0, 0, 5, 5).toPolygon, // 2
      Extent(2, 2, 8, 8).toPolygon, // 3
      Extent(5, 5, 10, 10).toPolygon, // 3
      Extent(10, 10, 20, 20).toPolygon // 3
    )

    val f: Array[Event[Point, None.type, String]] => Int = _.length

    //    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val countConverter = new Event2SpatialMapConverter(f, sArray)

    val tsRDD = countConverter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}