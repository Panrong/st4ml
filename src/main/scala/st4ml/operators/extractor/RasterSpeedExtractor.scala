package st4ml.operators.extractor

import org.apache.spark.rdd.RDD
import st4ml.instances.{Duration, Polygon, Raster, Trajectory}


class RasterSpeedExtractor extends Extractor[Raster[Polygon, Array[Trajectory[None.type, Map[String, String]]], None.type]] {
  type T = Raster[Polygon, Array[Trajectory[None.type, Map[String, String]]], None.type]
  type S = Polygon

  def extract(rasterRDD: RDD[T], metric: String = "euclidean", convertKmh: Boolean = false): RDD[((S, Duration), Double)] = {
    val calSpeed: Array[Trajectory[None.type, Map[String, String]]] => Double = trajArray => {
      if (trajArray.isEmpty) -1
      else {
        val speedArr = trajArray.map {
          traj =>
            traj.consecutiveSpatialDistance(metric).sum / traj.duration.seconds
        }
        val r = speedArr.sum / speedArr.length
        if (convertKmh) r * 3.6 else r
      }
    }
    val speedRDD = rasterRDD.map(raster => raster.mapValue(calSpeed))
    val aggregatedSpeed = speedRDD.map(raster =>
      raster.entries.map(_.value).zipWithIndex.map(_.swap)).flatMap(x => x)
      .groupByKey()
      .mapValues(x => {
        val arr = x.toArray.filter(_ > 0)
        if (arr.isEmpty) 0
        else arr.sum / arr.length
      })
    val sArray = rasterRDD.take(1).map(x => x.spatials zip x.temporals).head
    aggregatedSpeed.map(x => (sArray(x._1), x._2))
  }
}