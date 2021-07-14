package operatorsNew.extractor

import instances.{Duration, Instance, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class VITExtractor[T <: Trajectory[_, _] : ClassTag] extends Extractor[T] {

  def extract(rdd: RDD[T], speedThreshold: Double): RDD[(T, Array[Double])] = {
    rdd.filter(_.entries.length > 1).map(traj => {
      def calSpeed(spatialDistances: Array[Double], temporalDistances: Array[Long]): Array[Double] = {
        spatialDistances.zip(temporalDistances).map(x => x._1 / x._2)
      }

      val speedArr = traj.mapConsecutive(calSpeed)
      (traj, speedArr.filter(_ > speedThreshold))
    }
    )
  }

}

