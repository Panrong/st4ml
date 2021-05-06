package operators.convertion

import geometry.{Point, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Point2TrajConverter(timeSplit: Double = 600) extends Converter {

  override type I = Point
  override type O = Trajectory

  override def convert(rdd: RDD[Point]): RDD[Trajectory] = {
    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[Trajectory],
        classOf[Point]))
    val numPartitions = rdd.getNumPartitions
    rdd.map(p => (p.attributes("tripID"), p))
      .groupByKey()
      .coalesce(numPartitions)
      .mapValues(points => {
        points.toSeq.sortBy(_.timeStamp._1)
          .foldRight(new Array[Array[Point]](0)) {
            (x, r) =>
              if (r.length == 0 || x.timeStamp._1 - r.last.last.timeStamp._2 > timeSplit) r :+ Array(x)
              else {
                val last = r.last
                r.dropRight(1) :+ (last :+ x)
              }
          }
      })
      .flatMapValues(x => x)
      .map(_._2)
      .filter(_.length > 1)
      .zipWithUniqueId()
      .map { case (points, id) =>
        Trajectory(id.toString,
          points.head.timeStamp._1,
          points, attributes = Map("tripID" -> points.head.attributes("tripID")))
      }
  }
}
