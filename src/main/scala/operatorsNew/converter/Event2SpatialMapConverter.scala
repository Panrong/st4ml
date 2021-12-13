package operatorsNew.converter

import instances._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class Event2SpatialMapConverter(sArray: Array[Polygon],
                                override val optimization: String = "rtree"
                               ) extends Converter {

  val sMap: Array[(Int, Polygon)] = sArray.sortBy(x =>
    (x.getCoordinates.map(c => c.x).min, x.getCoordinates.map(c => c.y).min)).zipWithIndex.map(_.swap)

  var rTree: Option[RTree[Polygon]] = None

  lazy val smXMin: Double = sMap.head._2.getEnvelopeInternal.getMinX
  lazy val smYMin: Double = sMap.head._2.getEnvelopeInternal.getMinY
  lazy val smXMax: Double = sMap.last._2.getEnvelopeInternal.getMaxX
  lazy val smYMax: Double = sMap.last._2.getEnvelopeInternal.getMaxY
  lazy val smXLength: Double = sMap.head._2.getEnvelopeInternal.getMaxX - smXMin
  lazy val smYLength: Double = sMap.head._2.getEnvelopeInternal.getMaxY - smYMin
  lazy val smXSlots: Long = ((smXMax - smXMin) / smXLength).round
  lazy val smYSlots: Long = ((smYMax - smYMin) / smYLength).round

  def buildRTree(spatials: Array[Polygon]): RTree[Polygon] = {
    val r = math.sqrt(spatials.length).toInt
    val entries = spatials.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[Polygon](entries, r)
  }

  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](input: RDD[Event[S, V, D]]): RDD[SpatialMap[Polygon, Array[Event[S, V, D]], None.type]] = {
    type I = Event[S, V, D]
    type O = SpatialMap[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        Iterator(emptySm.attachInstance(events, events.map(_.extent.toPolygon)))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(sMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        emptySm.rTree = rTreeBc.value
        Iterator(emptySm.attachInstanceRTree(events, events.map(_.extent.toPolygon)))
      })
    }
    else if (optimization == "regular") {
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val xRanges = Range(math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots.toInt - 1, ((xMax - smXMin) / smXLength).toInt)).toArray
        val yRanges = Range(math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots.toInt - 1, ((yMax - smYMin) / smYLength).toInt)).toArray
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges) idRanges = idRanges :+ i * smYSlots.toInt + j
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = SpatialMap.empty[Polygon, I](sArray).sorted
          Iterator(emptySm.createSpatialMap(events))
        })
    }
    else throw new NoSuchElementException
  }

  // agg
  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag](input: RDD[Event[S, V, D]], agg: Array[Event[S, V, D]] => V2): RDD[SpatialMap[Polygon, V2, None.type]] = {
    type I = Event[S, V, D]
    type O = SpatialMap[Polygon, V2, None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        Iterator(emptySm.attachInstance(events, events.map(_.extent.toPolygon)).mapValue(agg))
      })
    }

    else if (optimization == "rtree") {
      rTree = Some(buildRTree(sMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val events = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        emptySm.rTree = rTreeBc.value
        Iterator(emptySm.attachInstanceRTree(events, events.map(_.extent.toPolygon)).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptySm = SpatialMap.empty[Polygon, I](sArray)
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val xRanges = Range(math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots.toInt - 1, ((xMax - smXMin) / smXLength).toInt)).toArray
        val yRanges = Range(math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots.toInt - 1, ((yMax - smYMin) / smYLength).toInt)).toArray
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges) idRanges = idRanges :+ i * smYSlots.toInt + j
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = SpatialMap.empty[Polygon, I](sArray).sorted
          Iterator(emptySm.createSpatialMap(events).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }


  // preMap + agg
  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag,
    S2 <: Geometry : ClassTag, V2: ClassTag, D2: ClassTag,
    V3: ClassTag](input: RDD[Event[S, V, D]], preMap: Event[S, V, D] => Event[S2, V2, D2],
                  agg: Array[Event[S2, V2, D2]] => V3):
  RDD[SpatialMap[Polygon, V3, None.type]] = {
    type I = Event[S2, V2, D2]
    type O = SpatialMap[Polygon, V3, None.type]
    if (optimization == "none") {
      input.map(preMap).mapPartitions(partition => {
        val events = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        Iterator(emptySm.attachInstance(events, events.map(_.extent.toPolygon)).mapValue(agg))
      })
    }

    else if (optimization == "rtree") {
      rTree = Some(buildRTree(sMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.map(preMap).mapPartitions(partition => {
        val events = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        emptySm.rTree = rTreeBc.value
        Iterator(emptySm.attachInstanceRTree(events, events.map(_.extent.toPolygon)).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptySm = SpatialMap.empty[Polygon, I](sArray)
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.map(preMap).flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val xRanges = Range(math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots.toInt - 1, ((xMax - smXMin) / smXLength).toInt)).toArray
        val yRanges = Range(math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots.toInt - 1, ((yMax - smYMin) / smYLength).toInt)).toArray
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges) idRanges = idRanges :+ i * smYSlots.toInt + j
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = SpatialMap.empty[Polygon, I](sArray).sorted
          Iterator(emptySm.createSpatialMap(events).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
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

    //    val sArray = Array(
    //      Extent(2, 2, 8, 8).toPolygon, // 3
    //      Extent(0, 0, 4, 4).toPolygon, // 2
    //      Extent(5, 5, 10, 10).toPolygon, // 3
    //      Extent(10, 10, 20, 20).toPolygon // 3
    //    )
    val sArray = Array(
      Extent(0, 0, 2, 2).toPolygon, // 1
      Extent(2, 2, 4, 4).toPolygon, // 1
      Extent(0, 2, 2, 4).toPolygon, // 1
      Extent(2, 0, 4, 2).toPolygon // 0
    )

    val f: Array[Event[Point, None.type, String]] => Int = _.length

    //    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val countConverter = new Event2SpatialMapConverter(sArray, "regular")

    val tsRDD = countConverter.convert(eventRDD, f).map(_.sorted)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}