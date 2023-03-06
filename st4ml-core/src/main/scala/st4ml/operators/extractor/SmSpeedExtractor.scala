package st4ml.operators.extractor

import st4ml.instances.{Polygon, SpatialMap, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SmSpeedExtractor[T <: SpatialMap[Polygon, Array[Trajectory[_, _]], _] : ClassTag] extends Extractor[T] {
  def extract(smRDD: RDD[T], metric: String = "euclidean", convertKmh: Boolean = false): Array[(Polygon, Double)] = {
    val calSpeed: Array[Trajectory[_, _]] => Double = trajArray => {
      if (trajArray.isEmpty) -1
      else {
        val speedArr = trajArray.map(traj =>
          traj.consecutiveSpatialDistance(metric).sum / traj.duration.seconds)
        val r = speedArr.sum / speedArr.length
        if (convertKmh) r / 3.6 else r
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