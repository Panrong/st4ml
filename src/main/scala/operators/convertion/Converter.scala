package operators.convertion

import geometry._
import geometry.road.RoadGrid
import operators.selection.partitioner.{QuadTreePartitioner, STRPartitioner, SpatialPartitioner, TemporalPartitioner}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class Converter {

  def traj2SpatialMap(rdd: RDD[(Int, mmTrajectory)]):
  RDD[SubSpatialMap[Array[(Long, String)]]] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[mmTrajectory],
        classOf[SubSpatialMap[Array[(Long, String)]]]))

    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      case (_, traj) =>
        traj.roads.map(x => (x, traj.id))
    }
      .map(x => (x._1._1, (x._1._2, x._2))) // (roadID, (timeStamp, trajID))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => SubSpatialMap(roadID = x._1, attributes = x._2))
  }

  def trajSpeed2SpatialMap(rdd: RDD[(Int, mmTrajectory)]):
  RDD[SubSpatialMap[Array[(Long, String, Double)]]] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[mmTrajectory],
        classOf[SubSpatialMap[Array[(Long, String, Double)]]]))

    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      case (_, traj) =>
        traj.roads.map(x => (x, traj.id)) zip traj.speed.map(_._2)
    }
      .map(x => (x._1._1._1, (x._1._1._2, x._1._2, x._2))) // (roadID, (timeStamp, trajID, speed))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => SubSpatialMap(roadID = x._1, attributes = x._2))
  }

  def traj2Point(rdd: RDD[(Int, Trajectory)]): RDD[Point] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[Trajectory],
        classOf[Point]))

    rdd.map(_._2).flatMap(
      traj => traj.points.map(p =>
        p.setAttributes(Map("tripID" -> traj.id))))
  }

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
  def point2SpatialMap(rdd: RDD[(Int, Point)],
                       roadMap: RoadGrid,
                       threshold: Double = 50): RDD[SubSpatialMap[Array[Point]]] = {
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
        case (edge, points) => SubSpatialMap(edge, points.toArray.distinct)
      }
  }

  /**
   * Convert points to multiple time series, where the dataset is spatially partitioned
   * and each partition consists of one time series.
   *
   * @param rdd          : point rdd after conversion
   * @param startTime    : start time of all time series
   * @param timeInterval : the length for temporal slicing
   * @param partitioner  : extends spatial partitioner
   * @tparam T : type of partitioner
   * @return : rdd of time series where each time series contains points attribute
   */
  def point2TimeSeries[T <: SpatialPartitioner : ClassTag]
  (rdd: RDD[(Int, Point)], startTime: Long, timeInterval: Int, partitioner: T)
  : RDD[TimeSeries[Point]] = {
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    repartitionedRDD.mapPartitions(partition => {
      val partitionID = partition.next()._1
      val points = partition.map(_._2).toArray
      val l = (points.map(_.t).max.toInt - startTime).toInt / timeInterval + 1
      val slots = Array.fill[Array[Point]](l)(new Array[Point](0))
      for (p <- points) {
        val s = ((p.t - startTime) / timeInterval).toInt
        slots(s) = slots(s) :+ p
      }

      val ts = TimeSeries(partitionID.toString, startTime, timeInterval, slots)
      Iterator(ts)
    })
  }

  /**
   * Convert points to one time series, which is temporally partitioned
   * and each partition consists of one sub time series.
   *
   *  For each time slot, the objects are left-inclusive
   *
   * @param rdd          : point rdd after conversion
   * @param startTime    : start time of the time series
   * @param timeInterval : the length for temporal slicing
   * @param endTime      : expected end time of the time series
   * @tparam T : type of partitioner
   * @return : rdd of time series where each time series contains points attribute
   */
  def point2TimeSeries[T <: SpatialPartitioner : ClassTag]
  (rdd: RDD[(Int, Point)], startTime: Long, timeInterval: Int, endTime: Option[Long] = None)
  : RDD[TimeSeries[Point]] = {
    val end = endTime.getOrElse(rdd.map(_._2.timeStamp._2).max)
    val numPartitions = rdd.getNumPartitions
    val partitioner = new TemporalPartitioner(startTime, end, timeInterval, numPartitions)
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    val numSlotsPerPartition = partitioner.numSlotsPerPartition

    repartitionedRDD.
      mapPartitions(partition => {
        if (partition.isEmpty) {
          Iterator(TimeSeries("Empty", startTime, timeInterval, new Array[Array[Point]](0)))
        } else {
          val partitionArray = partition.toArray
          val partitionID = partitionArray.head._1
          val partitionStartTime = startTime + partitionID * timeInterval * numSlotsPerPartition
          val points = partitionArray.map(_._2)
          val slots = Array.fill[Array[Point]](numSlotsPerPartition)(new Array[Point](0))
          for (p <- points) {
            val s = ((p.t - partitionStartTime) / timeInterval).toInt
            slots(s) = slots(s) :+ p
          }
          val ts = TimeSeries(partitionID.toString, partitionStartTime, timeInterval, slots)
          Iterator(ts)
        }
      }.filterNot(ts => ts.id == "Empty")
      )
  }

//  def traj2TimeSeries[T <: SpatialPartitioner : ClassTag]
//  (rdd: RDD[(Int, Trajectory)], startTime: Long, timeInterval: Int, partitioner: Option[T] = None)
//  : RDD[TimeSeries[Array[Trajectory]]] = {
//
//    if (partitioner.isDefined) {
//      val partitionedRDD = partitioner.get.partition(rdd.map(_._2))
//      partitionedRDD.mapPartitions(iter => {
//        if (iter.isEmpty) {
//          Iterator(TimeSeries("Empty", startTime, timeInterval, new Array[Array[Trajectory]](0)))
//        }
//        else {
//          val partitionerID = iter.next()._1
//          val ts = iter.map {
//            case (_, traj) => {
//              val l = (traj.points.last.t - startTime).toInt / timeInterval + 1
//              val slots = new Array[Array[Point]](l)
//              for (p <- traj.points) {
//                val s = ((p.t - startTime) / timeInterval).toInt
//                slots(s) = slots(s) :+ p
//              }
//              //            for (i <- 0 to slots.length - 2) {
//              //                slots(i) = slots(i) :+ slots(i + 1).head
//              //                slots(i + 1) = slots(i).last +: slots(i + 1)
//              //            }
//
//              // for temporary test
//              slots.dropRight(1).foreach(x => assert(x.length > 1))
//
//              slots.map(points => Trajectory(traj.tripID, points.head.t, points))
//            }
//          }
//          Iterator(TimeSeries(partitionerID.toString, startTime + partitionerID * timeInterval, timeInterval, ts.toArray))
//        }
//      })
//    }
//    else {
//      val slicedRDD = rdd.map(_._2).flatMap(traj => {
//        val l = (traj.points.last.t - startTime).toInt / timeInterval + 1
//        val slots = Array.fill[Array[Point]](l)(new Array[Point](0))
//        for (p <- traj.points) {
//          val s = ((p.t - startTime) / timeInterval).toInt
//          slots(s) = slots(s) :+ p
//        }
//        for (i <- 0 to slots.length - 2) {
//          slots(i) = slots(i) :+ slots(i + 1).head
//          slots(i + 1) = slots(i).last +: slots(i + 1)
//        }
//
//        // for temporary test
//        slots.dropRight(1).foreach(x => assert(x.length > 1))
//
//        slots.map(points => Trajectory(traj.tripID, points.head.t, points)).zipWithIndex
//          .map { case (traj, idx) => ((idx - 1) * timeInterval, traj) }
//
//      })
//      val series = slicedRDD.groupByKey().collect.sortBy(_._1).map(_._2.toArray)
//      val spark = SparkSession.builder().getOrCreate()
//      spark.sparkContext.parallelize(Seq(TimeSeries("traj", startTime, timeInterval, series)))
//    }
//  }
}
