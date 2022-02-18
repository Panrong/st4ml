package operatorsNew.extractor

import instances.{Duration, Geometry, Raster, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RasterSpeedExtractor[S <: Geometry, T <: Raster[S, Array[Trajectory[_, _]], _] : ClassTag] extends Extractor[T] {
  def extract(rasterRDD: RDD[T], metric: String = "euclidean", convertKmh: Boolean = false): Array[((S, Duration), Double)] = {
    val calSpeed: Array[Trajectory[_, _]] => Double = trajArray => {
      if (trajArray.isEmpty) -1
      else {
        val speedArr = trajArray.map(traj =>
          traj.consecutiveSpatialDistance(metric).sum / traj.duration.seconds)
        val r = speedArr.sum / speedArr.length
        if (convertKmh) r / 3.6 else r
      }
    }
    val speedRDD = rasterRDD.map(sm => sm.mapValue(calSpeed))
    val aggregatedSpeed = speedRDD.map(sm =>
      sm.entries.map(_.value).zipWithIndex.map(_.swap)).flatMap(x => x)
      .groupByKey()
      .mapValues(x => {
        val arr = x.toArray.filter(_ > 0)
        if (arr.isEmpty) 0
        else arr.sum / arr.length
      })
    val sArray = rasterRDD.take(1).map(x => x.spatials zip x.temporals).head
    aggregatedSpeed.collect.map(x => (sArray(x._1), x._2))
  }
}