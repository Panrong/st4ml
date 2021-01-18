package preprocessing

import geometry.{Point, Rectangle, Cube}


/** required case classes */
//case class Query2d(query: Rectangle, queryID: Long) extends Serializable
//case class Query3d(query: Cube, queryID: Long) extends Serializable
case class TrajectoryWithMBR(tripID: Long, startTime: Long, points: Array[Point],
                             mbr: Array[Double]) extends Serializable

case class TrajMBRQuery(tripID: Long, startTime: Long, points: Array[Point], mbr: Array[Double],
                        query: Rectangle) extends Serializable

case class resRangeQuery(queryID: Long, trips: List[Long], count: Long) extends Serializable

//case class mmTrajectory(tripID: Long,
//                        GPSPoints: Array[Point],
//                        VertexID: Map[String, Int],
//                        Candidates: Map[Int, Array[String]],
//                        PointRoadPair: Array[(Double, Double, String)],
//                        RoadTime: Array[(String, Long)]) extends Serializable
