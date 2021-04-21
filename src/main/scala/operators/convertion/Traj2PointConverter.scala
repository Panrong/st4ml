package operators.convertion

import geometry.{Point, Rectangle, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Traj2PointConverter(sRange: Option[Rectangle] = None, tRange: Option[(Long, Long)] = None) extends Converter {
  def convert(rdd: RDD[(Int, Trajectory)]): RDD[Point] = {
    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[Trajectory],
        classOf[Point]))

    val res1 = rdd.map(_._2).flatMap(
      traj => traj.points.map(p => {
        p.setAttributes(Map("tripID" -> traj.id))
      }))
    if (tRange.isEmpty) res1
    else {
      val res2 = res1.filter(x => {
        val (ts, te) = x.timeStamp
        ts <= tRange.get._2 && te >= tRange.get._1
      })
      if (sRange.isEmpty) res2
      else res2.filter(x => x.inside(sRange.get))
    }
  }
}

object Traj2PointConverter {
  def apply(sRange: Rectangle, tRange: (Long, Long)): Traj2PointConverter = {
    new Traj2PointConverter(Some(sRange), Some(tRange))
  }
}
