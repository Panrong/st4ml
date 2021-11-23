package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, RTree, SpatialMap}
import operators.selection.indexer.RTreeDeprecated
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Event2SpatialMapConverter[S <: Geometry, V, D, VSM, DSM](f: Array[Event[S, V, D]] => VSM,
                                                               sArray: Array[Polygon],
                                                               d: DSM = None) extends Converter {
  type I = Event[S, V, D]
  type O = SpatialMap[VSM, DSM]

  val sMap: Array[(Int, Polygon)] = sArray.sortBy(x =>
    (x.getCoordinates.map(c => c.x).min, x.getCoordinates.map(c => c.y).min)).zipWithIndex.map(_.swap)

  //  var rTreeDeprecated: Option[RTreeDeprecated[geometry.Rectangle]] = None
  var rTree: Option[RTree[Polygon]] = None

  lazy val smXMin: Double = sMap.head._2.getEnvelopeInternal.getMinX
  lazy val smYMin: Double = sMap.head._2.getEnvelopeInternal.getMinY
  lazy val smXMax: Double = sMap.last._2.getEnvelopeInternal.getMaxX
  lazy val smYMax: Double = sMap.last._2.getEnvelopeInternal.getMaxY
  lazy val smXLength: Double = sMap.head._2.getEnvelopeInternal.getMaxX - smXMin
  lazy val smYLength: Double = sMap.head._2.getEnvelopeInternal.getMaxY - smYMin
  lazy val smXSlots: Long = ((smXMax - smXMin) / smXLength).round
  lazy val smYSlots: Long = ((smYMax - smYMin) / smYLength).round

  //  def buildRTreeDeprecated(spatials: Array[Polygon]): RTreeDeprecated[geometry.Rectangle] = {
  //
  //    val r = math.sqrt(spatials.length).toInt
  //    val entries = spatials.map(s => {
  //      val e = Extent(s.getEnvelopeInternal)
  //      geometry.Rectangle(Array(e.xMin, e.yMin, e.xMax, e.yMax))
  //    }).zipWithIndex.map(x => (x._1, x._2.toString, x._2))
  //    RTreeDeprecated[geometry.Rectangle](entries, r)
  //  }

  def buildRTree(spatials: Array[Polygon]): RTree[Polygon] = {
    val r = math.sqrt(spatials.length).toInt
    val entries = spatials.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[Polygon](entries, r)
  }

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val events = partition.toArray
      val emptySm = SpatialMap.empty[I](sArray)
      Iterator(emptySm.attachInstance(events, events.map(_.extent.toPolygon))
        .mapValue(f)
        .mapData(_ => d))
    })
  }

  //  def convertWithRTreeDeprecated(input: RDD[I]): RDD[O] = {
  //    rTreeDeprecated = Some(buildRTreeDeprecated(sMap.map(_._2)))
  //    val spark = SparkSession.builder().getOrCreate()
  //    val rTreeBc = spark.sparkContext.broadcast(rTreeDeprecated)
  //    input.mapPartitions(partition => {
  //      val events = partition.toArray
  //      val emptySm = SpatialMap.empty[I](sArray)
  //      emptySm.rTreeDeprecated = rTreeBc.value
  //      Iterator(emptySm.attachInstanceRTreeDeprecated(events, events.map(_.extent.toPolygon))
  //        .mapValue(f)
  //        .mapData(_ => d))
  //    })
  //  }

  def convertWithRTree(input: RDD[I]): RDD[O] = {
    rTree = Some(buildRTree(sMap.map(_._2)))
    val spark = SparkSession.builder().getOrCreate()
    val rTreeBc = spark.sparkContext.broadcast(rTree)
    input.mapPartitions(partition => {
      val events = partition.toArray
      val emptySm = SpatialMap.empty[I](sArray)
      emptySm.rTree = rTreeBc.value
      Iterator(emptySm.attachInstanceRTree(events, events.map(_.extent.toPolygon))
        .mapValue(f)
        .mapData(_ => d))
    })
  }
  //  override def convert(input: RDD[I]): RDD[O] = {
  //    input.mapPartitions(partition => {
  //      val eventRegions = partition.map(event => {
  //        val regions = sMap.filter(_._2.intersects(event.extent))
  //        (event, regions.map(_._1))
  //      }).flatMap {
  //        case (event, slots) => slots.map(slot => (slot, event)).toIterator
  //      }.toArray.groupBy(_._1).mapValues(x => x.map(_._2)).toArray
  //      val entries = eventRegions.map(region => {
  //        val spatial = sMap(region._1)._2
  //        val temporal = durationAll(region._2)
  //        val v = f(region._2)
  //        new Entry(spatial, temporal, v)
  //      })
  //      val sm = new SpatialMap[VSM, DSM](entries, data = d)
  //      Iterator(sm)
  //    })
  //  }

  def convertRegular(input: RDD[I]): RDD[O] = {
    val emptySm = SpatialMap.empty[I](sArray)
    //assert(emptySm.isRegular, "The structure is not regular.")
    input.flatMap(e => {
      val xMin = e.extent.xMin
      val xMax = e.extent.xMax
      val yMin = e.extent.yMin
      val yMax = e.extent.yMax
      val xRanges = (((xMin - smXMin) / smXLength).toInt, ((xMax - smXMin) / smXLength).toInt)
      val yRanges = (((yMin - smYMin) / smYLength).toInt, ((yMax - smYMin) / smYLength).toInt)
      val idRanges = Range((xRanges._1 * smYSlots + yRanges._1).toInt, (xRanges._2 * smYSlots + yRanges._2).toInt + 1, 1).toArray
      idRanges.map(x => (e, x))
    })
      .mapPartitions(partition => {
        val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
        val emptySm = SpatialMap.empty[I](sArray)
        Iterator(emptySm.createSpatialMap(events)
          .mapValue(f)
          .mapData(_ => d))
      })
  }
}

object Event2SpatialMapConverter {
  def apply(sArray: Array[Polygon]): Event2SpatialMapConverter[Point,
    None.type, String, Array[Event[Point, None.type, String]], None.type] =
    new Event2SpatialMapConverter[Point, None.type, String, Array[Event[Point, None.type, String]], None.type](x => x, sArray)
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