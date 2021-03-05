package operators.convertion

import geometry._
import geometry.road.RoadGrid
import operators.selection.partitioner.{QuadTreePartitioner, STRPartitioner}
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
   * @param rdd : point RDD
   * @param roadMap : spatial Map
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
}
