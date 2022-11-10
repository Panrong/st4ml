package st4ml.operators.extractor

import st4ml.instances.Trajectory
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class VITExtractor[T <: Trajectory[_, _] : ClassTag] extends Extractor[T] {

  def extract(rdd: RDD[T], speedThreshold: Double): RDD[(T, Array[((Long, Long), Double)])] = {
    rdd.filter(_.entries.length > 1).map(traj => {
      def calSpeed(spatialDistances: Array[Double], temporalDistances: Array[Long]): Array[Double] = {
        spatialDistances.zip(temporalDistances).map(x => x._1 / x._2)
      }

      val speedArr = traj.mapConsecutive(calSpeed)
      val temporalArr = traj.temporalSliding(2)
        .map(x => (x(0).start, x(1).start))
        .toArray
      (traj, temporalArr.zip(speedArr).filter(_._2 > speedThreshold))
    }
    )
      .filter(_._2.length > 0)
  }
}

