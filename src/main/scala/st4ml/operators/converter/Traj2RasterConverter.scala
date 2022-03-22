package st4ml.operators.converter

import st4ml.instances.RoadNetwork.RoadNetwork
import st4ml.instances.{Duration, Entry, Geometry, LineString, Polygon, RTree, Raster, SpatialMap, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag


class Traj2RasterConverter(polygonArr: Array[Polygon],
                           durArr: Array[Duration],
                           override val optimization: String = "rtree") extends Converter {
  lazy val sMap: Array[(Int, Polygon)] = polygonArr.sortBy(x =>
    (x.getCoordinates.map(c => c.x).min, x.getCoordinates.map(c => c.y).min)).zipWithIndex.map(_.swap)
  lazy val tMap: Array[(Int, Duration)] = durArr.sortBy(_.start).zipWithIndex.map(_.swap)

  var rTree: Option[RTree[Polygon, String]] = None
  lazy val rasterXMin: Double = sMap.head._2.getEnvelopeInternal.getMinX
  lazy val rasterYMin: Double = sMap.head._2.getEnvelopeInternal.getMinY
  lazy val rasterXMax: Double = sMap.last._2.getEnvelopeInternal.getMaxX
  lazy val rasterYMax: Double = sMap.last._2.getEnvelopeInternal.getMaxY
  lazy val rasterXLength: Double = sMap.head._2.getEnvelopeInternal.getMaxX - rasterXMin
  lazy val rasterYLength: Double = sMap.head._2.getEnvelopeInternal.getMaxY - rasterYMin
  lazy val rasterXSlots: Int = ((rasterXMax - rasterXMin) / rasterXLength).round.toInt
  lazy val rasterYSlots: Int = ((rasterYMax - rasterYMin) / rasterYLength).round.toInt
  lazy val tsMin: Long = tMap.head._2.start
  lazy val tsMax: Long = tMap.last._2.end
  lazy val tsLength: Long = tMap.head._2.seconds
  lazy val tsSlots: Int = ((tsMax - tsMin) / tsLength).toInt

  def buildRTree(polygonArr: Array[Polygon],
                 durArr: Array[Duration]): RTree[Polygon, String] = {
    val r = (math.pow(polygonArr.length, 1.0 / 3) / 2).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- polygonArr.indices) {
      polygonArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (polygonArr(i).copy.asInstanceOf[Polygon], i.toString, i)
    }
    RTree[Polygon, String](entries, r, dimension = 3)
  }

  def convert[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]]): RDD[Raster[Polygon, Array[Trajectory[V, D]], None.type]] = {
    type I = Trajectory[V, D]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(trajs))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(trajs))
      })
    }
    else if (optimization == "regular") {
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      //      assert(emptyRaster.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val tMin = e.duration.start
        val tMax = e.duration.end
        val xRanges = Range(math.max(0, ((xMin - rasterXMin) / rasterXLength).toInt - 1), math.min(rasterXSlots - 1, ((xMax - rasterXMin) / rasterXLength).toInt) + 1)
        val yRanges = Range(math.max(0, ((yMin - rasterYMin) / rasterYLength).toInt - 1), math.min(rasterXSlots - 1, ((yMax - rasterYMin) / rasterYLength).toInt) + 1)
        val tRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt - 1), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        //        println(xMin, yMin, xMax, yMax, tMin, tMax, xRanges, yRanges, tRanges)
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges; k <- tRanges) {
          idRanges = idRanges :+ i * rasterYSlots + j + k * rasterYSlots * rasterXSlots
        }
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2, tMap(x)._2))
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
    V2: ClassTag, D2: ClassTag](input: RDD[Trajectory[V, D]], agg: Array[Trajectory[V, D]] => V2): RDD[Raster[Polygon, V2, None.type]] = {
    type I = Trajectory[V, D]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(trajs).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(trajs).mapValue(agg))
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
        val xRanges = Range(math.max(0, ((xMin - rasterXMin) / rasterXLength).toInt - 1), math.min(rasterXSlots - 1, ((xMax - rasterXMin) / rasterXLength).toInt) + 1)
        val yRanges = Range(math.max(0, ((yMin - rasterYMin) / rasterYLength).toInt - 1), math.min(rasterXSlots - 1, ((yMax - rasterYMin) / rasterYLength).toInt) + 1)
        val tRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt - 1), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        //        println(xMin, yMin, xMax, yMax, tMin, tMax, xRanges, yRanges, tRanges)
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges; k <- tRanges) {
          idRanges = idRanges :+ i * rasterYSlots + j + k * rasterYSlots * rasterXSlots
        }
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2, tMap(x)._2))
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

  def convert[V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag,
    V3: ClassTag](input: RDD[Trajectory[V, D]],
                  preMap: Trajectory[V, D] => Trajectory[V2, D2],
                  agg: Array[Trajectory[V2, D2]] => V3): RDD[Raster[Polygon, V3, None.type]] = {
    type I = Trajectory[V2, D2]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(trajs).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(trajs).mapValue(agg))
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
        val xRanges = Range(math.max(0, ((xMin - rasterXMin) / rasterXLength).toInt - 1), math.min(rasterXSlots - 1, ((xMax - rasterXMin) / rasterXLength).toInt) + 1)
        val yRanges = Range(math.max(0, ((yMin - rasterYMin) / rasterYLength).toInt - 1), math.min(rasterXSlots - 1, ((yMax - rasterYMin) / rasterYLength).toInt) + 1)
        val tRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt - 1), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges; k <- tRanges) {
          idRanges = idRanges :+ i * rasterYSlots + j + k * rasterYSlots * rasterXSlots
        }
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2, tMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val trajs = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)).map {
            case (id, instanceArr) =>
              (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
          }
          val emptySm = Raster.empty[I](polygonArr, durArr).sorted
          Iterator(emptySm.createRaster(trajs).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }

}