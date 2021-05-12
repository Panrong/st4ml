package operators.extraction

import geometry.{Rectangle, SpatialMap, Trajectory}
import org.apache.spark.rdd.RDD

class TrajSpatialMapExtractor[U] extends BaseExtractor[SpatialMap[Trajectory]] {
  def extractAvgSpeed(rdd: RDD[SpatialMap[Trajectory]]): RDD[((Long, Long), Map[Rectangle, Double])] = {
    rdd.map(sm => {
      (sm.timeStamp,
        sm.contents.toMap.mapValues(trajs => {
          if (trajs.length == 0) 0
          else trajs.map(traj => traj.calAvgSpeed()).sum / trajs.length
        }).map(x => x))
    })
  }

  def extractSpeed(rdd: RDD[SpatialMap[Trajectory]]): RDD[((Long, Long), Map[Rectangle, Array[Double]])] = {
    rdd.map(sm => {
      (sm.timeStamp,
        sm.contents.toMap.mapValues(trajs => {
          trajs.map(traj => traj.calAvgSpeed())
        }).map(x => x))
    })
  }

  def extractRangeAvgSpeed(rdd: RDD[SpatialMap[Trajectory]]): RDD[(Rectangle, Double)] = {
    val speedRDD = extractSpeed(rdd)
    speedRDD.map(_._2).map(_.toArray).flatMap(x=>x).map {
      case (rectangle, points) => (rectangle.coordinates.toList, points)
    }.reduceByKey(_ ++ _)
      .mapValues(x => if(x.length != 0) x.sum / x.length else 0)
      .map(x => (Rectangle(x._1.toArray), x._2))
  }
}
