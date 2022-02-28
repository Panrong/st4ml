package st4ml.operators.extractor

import st4ml.instances.{SpatialMap, Trajectory, Polygon}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RegionalSpeedExtractor[T <: SpatialMap[Polygon, Array[Trajectory[_, _]], _] : ClassTag] extends Extractor[T] {
  def extract(smRDD: RDD[T]): Array[(Polygon, Double)] = {

    val calSpeed: Array[Trajectory[_, _]] => Double = trajArray => {
      if (trajArray.isEmpty) -1
      else {
        val speedArr = trajArray.map(traj =>
          traj.consecutiveSpatialDistance("euclidean").sum / traj.duration.seconds)
        speedArr.sum / speedArr.length
      }
    }

    val speedRDD = smRDD.map(sm => sm.mapValue(calSpeed))

    val aggregatedSpeed = speedRDD.map(sm =>
      sm.entries.map(_.value).zipWithIndex.map(_.swap)).flatMap(x => x)
      .groupByKey()
      .mapValues(x => {
        val arr = x.toArray.filter(_ > 0)
        if (arr.isEmpty) 0
        else arr.sum / arr.length
      })
    val sArray = smRDD.take(1).map(x => x.spatials).head
    aggregatedSpeed.collect.map(x => (sArray(x._1), x._2))
  }
}