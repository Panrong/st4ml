package preprocessing

import geometry.{Point, Rectangle}


/** required case classes */
case class Query(query: Rectangle, queryID: Long) extends Serializable

case class TrajectoryWithMBR(tripID: Long, startTime: Long, points: Array[Point],
                             mbr: Array[Double]) extends Serializable

case class TrajMBRQuery(tripID: Long, startTime: Long, points: Array[Point], mbr: Array[Double],
                        query: Rectangle) extends Serializable

case class resRangeQuery(queryID: Long, trips: List[Long], count: Long) extends Serializable