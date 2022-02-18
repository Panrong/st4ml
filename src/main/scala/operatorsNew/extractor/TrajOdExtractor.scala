package operatorsNew.extractor

import instances.{Point, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TrajOdExtractor[T <: Trajectory[_, D] : ClassTag, D: ClassTag] extends Extractor[T] {
  def extract(rdd: RDD[T]): RDD[(Point, Point)] = {
    rdd.map { traj =>
      val arr = traj.entries.map(_.spatial)
      (arr.head, arr.last)
    }
  }
}
