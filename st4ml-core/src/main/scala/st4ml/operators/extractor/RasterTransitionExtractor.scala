package st4ml.operators.extractor

import st4ml.instances.{Duration, Entry, Geometry, Polygon, Raster, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 *
 * @param classTag$Traj$0
 * @tparam Traj : type of trajectories inside the raster, the raster has to have type Raster[Polygon, Array[Traj], None.type]
 */
class RasterTransitionExtractor[Traj <: Trajectory[_, _] : ClassTag] extends Extractor[Raster[Polygon, Array[Traj], None.type]] {
  type T = Raster[Polygon, Array[Traj], None.type]
  def extract(rasterRDD: RDD[T]): Raster[Polygon, (Int, Int), None.type] = {
    val resRDD = rasterRDD.map(raster => Raster(
      raster.entries.map(entry => {
        implicit val s: Polygon = entry.spatial
        implicit val t: Duration = entry.temporal
        Entry(entry.spatial, entry.temporal, findInOut(entry.value))}), None))
    val res = resRDD.collect
    def valueMerge(x: (Int, Int), y: (Int, Int)): (Int, Int) = (x._1 + y._1, x._2 + y._2)
    res.drop(1).foldRight(res.head)(_.merge(_, valueMerge, (_, _) => None))
  }
  def findInOut[T <: Trajectory[_, _]](arr: Array[T])(implicit sRange: Polygon, tRange: Duration): (Int, Int) = {
    val inOuts = arr.map { traj =>
      val inside = traj.entries.map(p => p.intersects(sRange, tRange)).sliding(2).toArray
      val in = inside.count(_ sameElements Array(false, true))
      val out = inside.count(_ sameElements Array(true, false))
      (in, out)
    }
    (inOuts.map(_._1).sum, inOuts.map(_._2).sum)
  }
}