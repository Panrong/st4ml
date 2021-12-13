package operatorsNew.converter

import instances.{Duration, Entry, Event, Extent, Geometry, Point, Polygon, RTree, Raster}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class Event2RasterConverter(polygonArr: Array[Polygon],
                            durArr: Array[Duration],
                            override val optimization: String = "rtree") extends Converter {
  lazy val sMap: Array[(Int, Polygon)] = polygonArr.sortBy(x =>
    (x.getCoordinates.map(c => c.x).min, x.getCoordinates.map(c => c.y).min)).zipWithIndex.map(_.swap)
  lazy val tMap: Array[(Int, Duration)] = durArr.sortBy(_.start).zipWithIndex.map(_.swap)

  var rTree: Option[RTree[Polygon]] = None
  lazy val rasterXMin: Double = sMap.head._2.getEnvelopeInternal.getMinX
  lazy val rasterYMin: Double = sMap.head._2.getEnvelopeInternal.getMinY
  lazy val rasterXMax: Double = sMap.last._2.getEnvelopeInternal.getMaxX
  lazy val rasterYMax: Double = sMap.last._2.getEnvelopeInternal.getMaxY
  lazy val rasterXLength: Double = sMap.head._2.getEnvelopeInternal.getMaxX - rasterXMin
  lazy val rasterYLength: Double = sMap.head._2.getEnvelopeInternal.getMaxY - rasterYMin
  lazy val rasterXSlots: Int = (((rasterXMax - rasterXMin) / rasterXLength).round).toInt
  lazy val rasterYSlots: Int = (((rasterYMax - rasterYMin) / rasterYLength).round).toInt
  lazy val tsMin: Long = tMap.head._2.start
  lazy val tsLength: Long = tMap.head._2.seconds
  lazy val tsSlots: Int = tMap.length

  def buildRTree(polygonArr: Array[Polygon],
                 durArr: Array[Duration]): RTree[Polygon] = {
    val r = math.sqrt(polygonArr.length).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- polygonArr.indices) {
      polygonArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (polygonArr(i).copy.asInstanceOf[Polygon], i.toString, i)
    }
    RTree[Polygon](entries, r, dimension = 3)
  }

  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](input: RDD[Event[S, V, D]]): RDD[Raster[Polygon, Array[Event[S, V, D]], None.type]] = {
    type I = Event[S, V, D]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(events))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(events))
      })
    }
    else if (optimization == "regular") {
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      assert(emptyRaster.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val tMin = e.duration.start
        val tMax = e.duration.end
        val xRanges = Range(math.max(0, ((xMin - rasterXMin) / rasterXLength).toInt), math.min(rasterXSlots - 1, ((xMax - rasterXMin) / rasterXLength).toInt))
        val yRanges = Range(math.max(0, ((yMin - rasterYMin) / rasterYLength).toInt), math.min(rasterXSlots - 1, ((yMax - rasterYMin) / rasterYLength).toInt))
        val tRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges; k <- tRanges) idRanges = idRanges :+ i * rasterYSlots + j + k * rasterYSlots * rasterXSlots
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = Raster.empty[I](polygonArr, durArr).sorted
          Iterator(emptySm.createRaster(events))
        })
    }
    else throw new NoSuchElementException
  }

  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag](input: RDD[Event[S, V, D]], agg: Array[Event[S, V, D]] => V2): RDD[Raster[Polygon, V2, None.type]] = {
    type I = Event[S, V, D]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(events).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(events).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      assert(emptyRaster.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val tMin = e.duration.start
        val tMax = e.duration.end
        val xRanges = Range(math.max(0, ((xMin - rasterXMin) / rasterXLength).toInt), math.min(rasterXSlots - 1, ((xMax - rasterXMin) / rasterXLength).toInt))
        val yRanges = Range(math.max(0, ((yMin - rasterYMin) / rasterYLength).toInt), math.min(rasterXSlots - 1, ((yMax - rasterYMin) / rasterYLength).toInt))
        val tRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges; k <- tRanges) idRanges = idRanges :+ i * rasterYSlots + j + k * rasterYSlots * rasterXSlots
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = Raster.empty[I](polygonArr, durArr).sorted
          Iterator(emptySm.createRaster(events).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }

  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag,
    S2 <: Geometry : ClassTag, V2: ClassTag, D2: ClassTag,
    V3: ClassTag](input: RDD[Event[S, V, D]],
                  preMap: Event[S, V, D] => Event[S2, V2, D2],
                  agg: Array[Event[S2, V2, D2]] => V3): RDD[Raster[Polygon, V3, None.type]] = {
    type I = Event[S2, V2, D2]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.map(preMap).mapPartitions(partition => {
        val events = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(events).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.map(preMap).mapPartitions(partition => {
        val events = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(events).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      assert(emptyRaster.isRegular, "The structure is not regular.")
      input.map(preMap).flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val tMin = e.duration.start
        val tMax = e.duration.end
        val xRanges = Range(math.max(0, ((xMin - rasterXMin) / rasterXLength).toInt), math.min(rasterXSlots - 1, ((xMax - rasterXMin) / rasterXLength).toInt))
        val yRanges = Range(math.max(0, ((yMin - rasterYMin) / rasterYLength).toInt), math.min(rasterXSlots - 1, ((yMax - rasterYMin) / rasterYLength).toInt))
        val tRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges; k <- tRanges) idRanges = idRanges :+ i * rasterYSlots + j + k * rasterYSlots * rasterXSlots
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = Raster.empty[I](polygonArr, durArr).sorted
          Iterator(emptySm.createRaster(events).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
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

    //    val stArray = Array(
    //      (Extent(0, 0, 5, 5).toPolygon, Duration(0, 100)),
    //      (Extent(2, 2, 8, 8).toPolygon, Duration(100, 200)),
    //      (Extent(5, 5, 10, 10).toPolygon, Duration(200, 300)),
    //      (Extent(10, 10, 20, 20).toPolygon, Duration(300, 400))
    //    )
    val stArray = Array(
      (Extent(0, 0, 5, 5).toPolygon, Duration(0, 100)),
      (Extent(5, 5, 10, 10).toPolygon, Duration(0, 100)),
      (Extent(5, 0, 10, 5).toPolygon, Duration(0, 100)),
      (Extent(0, 5, 5, 10).toPolygon, Duration(0, 100)),
      (Extent(0, 0, 5, 5).toPolygon, Duration(100, 200)),
      (Extent(5, 5, 10, 10).toPolygon, Duration(100, 200)),
      (Extent(5, 0, 10, 5).toPolygon, Duration(100, 200)),
      (Extent(0, 5, 5, 10).toPolygon, Duration(100, 200))
    )
    val f: Array[Event[Point, None.type, String]] => Int = _.length

    //    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val converter = new Event2RasterConverter(stArray.map(_._1), stArray.map(_._2))

    val tsRDD = converter.convert(eventRDD).map(_.sorted)
    tsRDD.collect.foreach(println(_))


    sc.stop()
  }
}
