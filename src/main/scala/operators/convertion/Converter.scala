package operators.convertion

import geometry._
import geometry.road.RoadGrid
import operators.selection.partitioner.{STRPartitioner, SpatialPartitioner, TemporalPartitioner}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._
import scala.math.{max, min}

import scala.reflect.ClassTag

class Converter extends Serializable {

  def traj2RoadMap(rdd: RDD[(Int, mmTrajectory)]):
  RDD[RoadMap[Array[(Long, String)]]] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[mmTrajectory],
        classOf[RoadMap[Array[(Long, String)]]]))

    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      case (_, traj) =>
        traj.roads.map(x => (x, traj.id))
    }
      .map(x => (x._1._1, (x._1._2, x._2))) // (roadID, (timeStamp, trajID))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => RoadMap(roadID = x._1, attributes = x._2))
  }

  def trajSpeed2RoadMap(rdd: RDD[(Int, mmTrajectory)]):
  RDD[RoadMap[Array[(Long, String, Double)]]] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[mmTrajectory],
        classOf[RoadMap[Array[(Long, String, Double)]]]))

    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      case (_, traj) =>
        traj.roads.map(x => (x, traj.id)) zip traj.speed.map(_._2)
    }
      .map(x => (x._1._1._1, (x._1._1._2, x._1._2, x._2))) // (roadID, (timeStamp, trajID, speed))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => RoadMap(roadID = x._1, attributes = x._2))
  }

  def traj2Point(rdd: RDD[(Int, Trajectory)]): RDD[Point] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[Trajectory],
        classOf[Point]))

    rdd.map(_._2).flatMap(
      traj => traj.points.map(p => {
        p.setAttributes(Map("tripID" -> traj.id))
      }))
  }

  def traj2Point(rdd: RDD[(Int, Trajectory)], tRange: (Long, Long), sRange: Rectangle): RDD[Point] =
    traj2Point(rdd).filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tRange._2 && te >= tRange._1
    })
      .filter(x => x.inside(sRange))

  def traj2PointClean(rdd: RDD[(Int, Trajectory)], sQuery: Rectangle): RDD[Point] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[Trajectory],
        classOf[Point]))

    rdd.map(_._2).flatMap(
      traj => traj.points.map(p =>
        p.setAttributes(Map("tripID" -> traj.id))))
      .filter(_.inside(sQuery))
  }

  def doNothing[T <: Shape : ClassTag](rdd: RDD[(Int, T)]): RDD[T] = {
    rdd.map(_._2)
  }

  def point2Traj(rdd: RDD[(Int, Point)], timeSplit: Double = 600): RDD[Trajectory] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[Trajectory],
        classOf[Point]))

    val numPartitions = rdd.getNumPartitions
    rdd.map(p => (p._2.attributes("tripID"), p._2))
      .groupByKey()
      .coalesce(numPartitions)
      .mapValues(points => {
        points.toSeq.sortBy(_.timeStamp._1)
          .foldRight(new Array[Array[Point]](0)) {
            (x, r) =>
              if (r.length == 0 || x.timeStamp._1 - r.last.last.timeStamp._2 > timeSplit) r :+ Array(x)
              else {
                val last = r.last
                r.dropRight(1) :+ (last :+ x)
              }
          }
      })
      .flatMapValues(x => x)
      .map(_._2)
      .filter(_.length > 1)
      .zipWithUniqueId()
      .map { case (points, id) =>
        Trajectory(id.toString,
          points.head.timeStamp._1,
          points, attributes = Map("tripID" -> points.head.attributes("tripID")))
      }
  }

  /**
   * map GPS points to spatial map based on shortest distance
   *
   * @param rdd       : point RDD
   * @param roadMap   : spatial Map
   * @param threshold : maximum projection distance from a point to a road for matching
   * @return : SpatialMap RDD
   */
  def point2RoadMap(rdd: RDD[(Int, Point)],
                    roadMap: RoadGrid,
                    threshold: Double = 50): RDD[RoadMap[Array[Point]]] = {
    val sc = SparkContext.getOrCreate()
    val numPartitions = rdd.getNumPartitions
    val eRDD = sc.parallelize(roadMap.edges, numPartitions)
    val partitioner = new STRPartitioner(numPartitions, threshold = threshold, samplingRate = Some(1))
    val (pointRDD, edgeRDD) = partitioner.copartition(rdd.map(_._2), eRDD)
    val pointEdgeDistRDD = pointRDD.zipPartitions(edgeRDD)((iterator1, iterator2) => {
      val points = iterator1.map(_._2).toArray
      val edges = iterator2.map(_._2).toArray
      points.flatMap(
        point => edges.map(edge => (point, (edge.id, edge.minDist(point)))))
        .filter { case (_, (_, d)) => d < threshold }
        .toIterator
    })
    val pointEdgeRDD = pointEdgeDistRDD.map(x => (x._1.id, x)).reduceByKey((a, b) => List(a, b).minBy(_._2._2))
      .map(_._2)
      .map {
        case (point, (edge, _)) => (edge, point)
      }
    pointEdgeRDD.groupByKey()
      .map {
        case (edge, points) => RoadMap(edge, points.toArray.distinct)
      }
  }

  /**
   * Convert points to timeSeries, where the dataset is spatially partitioned
   * and each partition consists of one time series.
   *
   * @param rdd          : point rdd after conversion
   * @param startTime    : start time of all time series
   * @param timeInterval : the length for temporal slicing
   * @param partitioner  : extends spatial partitioner
   * @tparam T : type of partitioner
   * @return : rdd of timeSeries
   */
  def point2TimeSeries[T <: SpatialPartitioner : ClassTag]
  (rdd: RDD[(Int, Point)], startTime: Long, timeInterval: Int, partitioner: T)
  : RDD[TimeSeries[Point]] = {
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    val pointsPerPartition = repartitionedRDD.mapPartitions(iter => Iterator(iter.length)).collect
    println(s"... Number of points per partition: " + s"${pointsPerPartition.deep}")
    repartitionedRDD.mapPartitions(partition =>
      if (partition.isEmpty) {
        Iterator(TimeSeries[Point]("Empty", startTime, timeInterval, Rectangle(Array(0, 0, 0, 0)), new Array[Array[Point]](0)))
      } else {
        val slotMap = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Point]]()
        var partitionID = 0
        while (partition.hasNext) {
          val (i, point) = partition.next()
          val slot = ((point.timeStamp._1 - startTime) / timeInterval).toInt
          slotMap += ((slot, if (slotMap.contains(slot)) slotMap(slot) ++ Array(point) else scala.collection.mutable.ArrayBuffer(point)))
          partitionID = i
        }
        val l = slotMap.keys.max
        val empty = Array.fill[Array[Point]](l)(new Array[Point](0)).zipWithIndex.map(_.swap).toMap // take positions of empty slots
        val slots = (empty ++ slotMap.mapValues(_.toArray)).toArray.sortBy(_._1).map(_._2)

        //        /** takes more memory */
        //        val partitionArray = partition.toArray
        //        val partitionID = partitionArray.head._1
        //        val points = partitionArray.map(_._2)
        //        val l = (points.map(_.t).max.toInt - startTime).toInt / timeInterval + 1
        //        val slotsMap = points.map(p => (((p.t - startTime) / timeInterval).toInt, p)).groupBy(_._1).mapValues(_.map(_._2))
        //        val empty = Array.fill[Array[Point]](l)(new Array[Point](0)).zipWithIndex.map(_.swap)
        //        val slots = (slotsMap.toArray ++ empty).groupBy(_._1).toArray.sortBy(_._1).map(_._2.head._2)

        val spatialRange = partitioner.partitionRange(partitionID)
        val ts = TimeSeries(partitionID.toString, startTime, timeInterval, spatialRange, slots)
        Iterator(ts)
      }).filter(x => x.id != "Empty")
  }

  // partition the whole space into cells, find sub-trajectories inside each cell, grouping as time series
  def traj2TimeSeries[T <: SpatialPartitioner : ClassTag]
  (rdd: RDD[(Int, Trajectory)], startTime: Long, timeInterval: Int, partitioner: T): RDD[TimeSeries[Trajectory]] = {
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    val partitionRange = partitioner.partitionRange
    repartitionedRDD.map(x => (x._1, x._2.windowBy(partitionRange(x._1)))).mapPartitions(partition =>
      if (partition.isEmpty) {
        Iterator(TimeSeries[Trajectory]("Empty", startTime, timeInterval, Rectangle(Array(0, 0, 0, 0)), new Array[Array[Trajectory]](0)))
      } else {
        val partitionArray = partition.toArray
        val partitionID = partitionArray.head._1
        val spatialRange = partitioner.partitionRange(partitionID)
        val trajs = partitionArray.map(_._2).filter(_.isDefined).flatMap(_.get)
        val l = (trajs.map(_.endTime._2).max - startTime).toInt / timeInterval + 1
        val slotsMap = trajs.flatMap(traj => {
          val slots = ((traj.startTime - startTime) / timeInterval).toInt to ((traj.endTime._2 - startTime) / timeInterval).toInt
          slots.toArray.map((_, traj))
        }).groupBy(_._1).mapValues(_.map(_._2))
        val empty = Array.fill[Array[Trajectory]](l)(new Array[Trajectory](0)).zipWithIndex.map(_.swap)
        val slots = (slotsMap.toArray ++ empty).groupBy(_._1).toArray.sortBy(_._1).map(_._2.head._2)
        val ts = TimeSeries(partitionID.toString, startTime, timeInterval, spatialRange, slots)
        Iterator(ts)
      }).filter(x => x.id != "Empty")
  }

  // convert the raster elements inside one partition to one spatial map
  def raster2SpatialMap[I: ClassTag, T: ClassTag](rdd: RDD[(Int, Raster[T])]): RDD[SpatialMap[T]] = {
    rdd.map(_._2).mapPartitions(iter => {
      val rasters = iter.toArray
      val combinedRaster = rasters.head.aggregateTemporal(rasters.drop(1))
      val contents = combinedRaster.contents.flatMap(x => x.series).flatten
      Iterator(SpatialMap(id = combinedRaster.id,
        timeStamp = combinedRaster.temporalRange(),
        contents = Array((combinedRaster.spatialRange(), contents))))
    })
  }

  // convert the raster elements inside one partition to one time series

  def raster2TimeSeries[I: ClassTag, T: ClassTag](rdd: RDD[(Int, Raster[T])]): RDD[TimeSeries[T]] = rdd.map(_._2).mapPartitions(
    iter => {
      val rasters = iter.toArray
      val combinedRaster = rasters.head.aggregateSpatial(rasters.drop(1))
      val contents = combinedRaster.contents.flatMap(x => x.series).flatten
      val temporalRange = combinedRaster.temporalRange()
      Iterator(TimeSeries(id = combinedRaster.id,
        startTime = temporalRange._1,
        timeInterval = (temporalRange._2 - temporalRange._1).toInt,
        spatialRange = combinedRaster.spatialRange(), series = Array(contents)))
    })

  def point2SpatialMap(rdd: RDD[(Int, Point)],
                       startTime: Long,
                       endTime: Long,
                       regions: Map[Int, Rectangle],
                       timeInterval: Option[Int] = None): RDD[SpatialMap[Point]] = {
    val numPartitions = if (timeInterval.isEmpty) rdd.getNumPartitions
    else (endTime - startTime).toInt / timeInterval.get + 1
    val partitioner = new TemporalPartitioner(startTime, endTime, numPartitions = numPartitions)
    val duration = timeInterval.getOrElse((endTime - startTime).toInt / numPartitions + 1)
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    repartitionedRDD.mapPartitionsWithIndex {
      case (partitionID, iter) =>
        val pointsArray = iter.toArray
        val points = pointsArray.map(_._2)
        var regionMap = regions.map((_, new Array[Point](0)))
        val boundary = genBoundary(regions)
        for (p <- points) {
          breakable {
            for (k <- regionMap.keys) {
              val pointShrink = Point(Array(
                min(max(p.x, boundary.head), boundary(2)),
                min(max(p.y, boundary(1)), boundary(3))
              ))
              if (pointShrink.inside(k._2)) {
                val o = regionMap(k)
                regionMap = regionMap + (k -> (o :+ p))
                break()
              }
            }
          }
        }
        val regionContents = regionMap.toArray.sortBy(_._1._1).map(x => (x._1._2, x._2))
        Iterator(SpatialMap(id = partitionID.toString,
          timeStamp = (startTime + partitionID * duration, startTime + (partitionID + 1) * duration),
          contents = regionContents))
    }
  }

  def traj2SpatialMap(rdd: RDD[(Int, Trajectory)],
                      startTime: Long,
                      endTime: Long,
                      regions: Map[Int, Rectangle],
                      timeInterval: Option[Int] = None): RDD[SpatialMap[Trajectory]] = {
    val numPartitions = if (timeInterval.isEmpty) rdd.getNumPartitions
    else (endTime - startTime).toInt / timeInterval.get + 1
    val duration = timeInterval.getOrElse((endTime - startTime).toInt / numPartitions + 1)
    val timeRanges = (0 until numPartitions).map(x => (startTime + x * duration, startTime + (x + 1) * duration)).toArray
    val partitioner = new TemporalPartitioner(startTime, endTime, numPartitions = numPartitions)
    val repartitionedRDD = partitioner.partitionToMultiple(rdd.map(_._2))
    repartitionedRDD.map(traj => (traj._1, traj._2.windowBy(timeRanges(traj._1)))).mapPartitions(partition => {
      if (partition.isEmpty) {
        Iterator(SpatialMap[Trajectory]("Empty", (startTime, endTime), new Array[(Rectangle, Array[Trajectory])](0)))
      } else {
        val iterArray = partition.toArray
        val partitionID = iterArray.head._1
        val trajs = iterArray.map(_._2).filter(_.isDefined).flatMap(_.get)
        val contents = trajs
          .flatMap(traj => regions.values.filter(x => x.intersect(traj.mbr)).map((_, traj)))
          .groupBy(_._1)
          .map(x => (x._1, x._2.map(_._2)))
          .toArray
        Iterator(SpatialMap(id = timeRanges(partitionID).toString, timeStamp = timeRanges(partitionID), contents))
      }
    }).filter(x => x.id != "Empty")
  }

  def timeSeries2Raster[T <: Shape : ClassTag](rdd: RDD[(Int, TimeSeries[T])], timeInterval: Int): RDD[Raster[T]] = {
    rdd.map(_._2).mapPartitions(iter => {
      val ts = iter.toArray.head
      val subTimeSeries = ts.splitByInterval(timeInterval)
      subTimeSeries.zipWithIndex.map {
        case (x, id) =>
          Raster(id = ts.id + "-" + id.toString, Array(x))
      }.toIterator
    })
  }

  def spatialMap2Raster[T <: Shape : ClassTag]
  (rdd: RDD[(Int, SpatialMap[T])]): RDD[Raster[T]] = {
    rdd.map(_._2).mapPartitions(iter => {
      val sm = iter.toArray.head
      val subSpatialMaps = sm.splitByCapacity(1)
      subSpatialMaps.zipWithIndex.map {
        case (x, id) =>
          val ts = TimeSeries(id.toString,
            startTime = x.startTime,
            timeInterval = (x.endTime - x.startTime).toInt,
            spatialRange = x.contents.head._1,
            series = Array(x.contents.flatMap(_._2)))
          Raster(id = sm.id + "-" + id.toString, Array(ts))
      }.toIterator
    })
  }

  def spatialMap2Point(rdd: RDD[(Int, SpatialMap[Point])]): RDD[Point] = {
    rdd.map(_._2).flatMap(_.contents).flatMap(x => x._2)
  }

  def timeSeries2Point(rdd: RDD[(Int, TimeSeries[Point])]): RDD[Point] = {
    rdd.map(_._2).flatMap(_.series).flatMap(x => x)
  }

  //one spatial contains several spatial regions, each region converts to a time series (of one temporal slot)
  def spatialMap2TimeSeries[T: ClassTag](rdd: RDD[(Int, SpatialMap[T])]): RDD[TimeSeries[T]] = {
    rdd.map(_._2).map(sm => {
      val regions = sm.contents
      val startTime = sm.startTime
      val timeInterval = (sm.endTime - sm.startTime).toInt
      regions.zipWithIndex.map {
        case ((spatialRange, contents), id) => TimeSeries(id.toString, startTime, timeInterval, spatialRange, Array(contents))
      }
    }).flatMap(x => x)
  }

  //one time series contains several temporal slots, each slot converts to a spatial map (of one spatial region)
  def timeSeries2SpatialMap[T: ClassTag](rdd: RDD[(Int, TimeSeries[T])]): RDD[SpatialMap[T]] = {
    rdd.map(_._2).map(ts => {
      val slots = ts.series
      val timeStamp = (ts.startTime, ts.endTime)
      val spatialRange = ts.spatialRange
      slots.zipWithIndex.map {
        case (contents, id) => SpatialMap(id.toString, timeStamp, Array((spatialRange, contents)))
      }
    }).flatMap(x => x)
  }

  /** helper functions * */

  def genBoundary(partitionMap: Map[Int, Rectangle]): Array[Double] = {
    val boxes = partitionMap.values.map(_.coordinates)
    val minLon = boxes.map(_.head).min
    val minLat = boxes.map(_ (1)).min
    val maxLon = boxes.map(_ (2)).max
    val maxLat = boxes.map(_.last).max
    Array(minLon, minLat, maxLon, maxLat)
  }
}