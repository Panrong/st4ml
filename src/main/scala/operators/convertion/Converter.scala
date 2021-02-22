package operators.convertion

import geometry._
import org.apache.spark.rdd.RDD

class Converter {
  def traj2SpatialMap(rdd: RDD[(Int, mmTrajectory)]):
  RDD[subSpatialMap[Array[(Long, String)]]] = {
    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      case (_, traj) =>
        traj.roads.map(x => (x, traj.id))
    }
      .map(x => (x._1._1, (x._1._2, x._2))) // (roadID, (timeStamp, trajID))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => subSpatialMap(roadID = x._1, attributes = x._2))
  }

  def trajSpeed2SpatialMap(rdd: RDD[(Int, mmTrajectory)]):
  RDD[subSpatialMap[Array[(Long, String, Double)]]] = {
    val numPartitions = rdd.getNumPartitions
    rdd.flatMap {
      case (_, traj) =>
        traj.roads.map(x => (x, traj.id)) zip traj.speed.map(_._2)
    }
      .map(x => (x._1._1._1, (x._1._1._2, x._1._2, x._2))) // (roadID, (timeStamp, trajID, speed))
      .groupByKey(numPartitions).mapValues(_.toArray)
      .map(x => subSpatialMap(roadID = x._1, attributes = x._2))
  }

  def traj2Point(rdd: RDD[(Int, Trajectory)]): RDD[Point] = {
    rdd.map(_._2).flatMap(
      traj => traj.points.map(p =>
        p.setAttributes(Map("tripID"->traj.id))))
  }
}
