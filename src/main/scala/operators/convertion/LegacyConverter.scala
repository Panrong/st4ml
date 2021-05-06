package operators.convertion

import geometry._
import geometry.road.RoadGrid
import operators.selection.partitioner.STRPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


class LegacyConverter extends Serializable {

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

  def trajSpeed2RoadMap(rdd: RDD[mmTrajectory]):
  RDD[RoadMap[Array[(Long, String, Double)]]] = {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[mmTrajectory],
        classOf[RoadMap[Array[(Long, String, Double)]]]))

    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      traj =>
        traj.roads.map(x => (x, traj.id)) zip traj.speed.map(_._2)
    }
      .map(x => (x._1._1._1, (x._1._1._2, x._1._2, x._2))) // (roadID, (timeStamp, trajID, speed))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => RoadMap(roadID = x._1, attributes = x._2))
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


}