package main.scala.rangequery

import org.apache.spark.Partitioner
import main.scala.mapmatching.SpatialClasses._

class rangeQuery {

}

case class keyPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key.toString.toInt % numParts
}
