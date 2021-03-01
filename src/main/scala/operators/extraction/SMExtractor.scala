package operators.extraction

import geometry.subSpatialMap
import org.apache.spark.rdd.RDD
import scalaz.Scalaz._

class SMExtractor extends Extractor with Serializable {
  /**
   * Get average speed for each road of the whole dataset
   *
   * @param rdd : road with attribute of (timeStamp, trajID, speed))
   * @return Map of (roadID, avgSpeed)
   */
  def extractRoadSpeed(rdd: RDD[subSpatialMap[Array[(Long, String, Double)]]]):
  Map[String, Double] = {
    rdd.map(x => (x.roadID, x.attributes)).mapValues(x => x.head._3)
      .mapValues((_, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect.toMap
  }

  /**
   * Get flow for each road of the whole dataset
   *
   * @param rdd : road with attribute of (timeStamp, trajID, speed))
   * @return Map of (roadID, avgSpeed)
   */
  def extractRoadFlow(rdd: RDD[subSpatialMap[Array[(Long, String, Double)]]]): Map[String, Int] = {
    rdd.map(x => (x.roadID, x.attributes)).mapValues(x => x.length)
      .collect.toMap
  }

  /**
   * Extract flow per road segment per time interval
   *
   * @param rdd        : road with attribute of (timeStamp, trajID, speed))
   * @param startTime  : Long
   * @param windowSize : in second
   * @return : Map of ( roadID -> (flow1, flow2, ...))
   */
  def extractSlidingFlow(rdd: RDD[subSpatialMap[Array[(Long, String, Double)]]],
                         startTime: Long, endTime: Long, windowSize: Int):
  Map[String, List[Int]] = {
    val length = ((endTime - startTime) / windowSize).toInt + 1
    val emptyArray = scala.collection.mutable.ArrayBuffer.fill(length)(0)
    rdd.map(x => (x.roadID, x.attributes))
      .mapValues(x => x.map(s => (s._1 - startTime).toInt / windowSize))
      .mapValues(x => x.groupBy(identity).mapValues(_.size))
      .mapValues(x => x.filter { case (k, _) => k < length})
      .mapValues(x => {
        x.foreach {
          case (k, v) => emptyArray(k) = v
        }
        emptyArray.toList
      })
      .collect.toMap
  }

  def extractSlidingSpeed(rdd: RDD[subSpatialMap[Array[(Long, String, Double)]]],
                          startTime: Long, endTime: Long, windowSize: Int):
  Map[String, List[Double]] = {

    val length = ((endTime - startTime) / windowSize).toInt + 1
    val emptyArray = scala.collection.mutable.ArrayBuffer.fill(length)(0.0)
    rdd.map(x => (x.roadID, x.attributes))
      .mapValues(x => x.map(s => ((s._1 - startTime).toInt / windowSize, s._3)))
      .mapValues(x => x.groupBy(_._1).mapValues(_.map(_._2)).mapValues(x => x.sum / x.length.toDouble))
      .mapValues(x => x.filter { case (k, _) => k < length})
        .mapValues(x => {
          x.foreach {
            case (k, v) => emptyArray(k) = v
          }
          emptyArray.toList
        })
        .collect.toMap

  }
}

