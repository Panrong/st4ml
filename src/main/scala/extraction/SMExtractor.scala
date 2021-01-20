package extraction

import geometry.subSpatialMap
import org.apache.spark.rdd.RDD

class SMExtractor {

  def extractRoadSpeed(rdd: RDD[subSpatialMap[Array[(Long, Long, Double)]]]):
  Map[String, Double] =
    rdd.map(x => (x.roadID, x.attributes)).mapValues(x => x.head._3)
      .mapValues((_, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect().toMap
}
