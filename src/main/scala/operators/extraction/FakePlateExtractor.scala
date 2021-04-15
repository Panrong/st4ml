package operators.extraction

import geometry.Trajectory
import org.apache.spark.rdd.RDD

class FakePlateExtractor extends Extractor {
  def extract(rdd: RDD[Trajectory], speedThreshold: Double): RDD[String] = {
    rdd.filter(traj => traj.hasFakePlate(speedThreshold)).map(_.id)
  }

  def extractAndShowDetail(rdd: RDD[Trajectory], speedThreshold: Double): RDD[(String, Array[(Long, Double)])] = {
    rdd.map(x => (x.id, x.findAbnormalSpeed(speedThreshold))).filter(_._2.nonEmpty)
  }
}

