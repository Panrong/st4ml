//package stInstance
//
//import org.scalatest.funsuite.AnyFunSuite
//import STInstance.{Point, SpatialMap, Trajectory}
//
//class implicitsSuite extends AnyFunSuite{
//
//  val r = new scala.util.Random
//  var coords = new Array[((Double, Double), Long)](0)
//  var roadIDs = new Array[(String, Long)](0)
//  val t = 0
//  for (i <- 0 to 10) {
//    coords = coords :+ ((r.nextDouble(), r.nextDouble()), (t + i).toLong)
//    roadIDs = roadIDs :+ (r.nextInt(1000).toString, (t + i).toLong)
//  }
//  val traj: Trajectory[(Double, Double), Null] = Trajectory[(Double, Double), Null](
//    id = 0,
//    coords,
//    property = None
//  )
//
//  val trajRoadNet: Trajectory[String, Null] = Trajectory[String, Null](
//    id = 0,
//    roadIDs,
//    property = None
//  )
//
//  test("STInstances: trajectory points conversion") {
//    import STInstance.implicits._
//    val points: Array[Point[Long]] = traj
//    points.foreach(x => println(x))
//  }
//
//  test("STInstances: trajectory SM conversion") {
//    import STInstance.implicits._
//    val vertices: Map[String, (Long, Long)] = trajRoadNet
//    vertices.foreach(x => println(x))
//    println(vertices)
//    val spatialMapFromVertices: SpatialMap[(Long, Long), Null] = vertices
//    val spatialMapFromTraj: SpatialMap[(Long, Long), Null] = trajRoadNet
//    assert(spatialMapFromVertices == spatialMapFromTraj)
//    println(spatialMapFromVertices)
//  }
//
////  test("Instance geometry conversion") {
////    import STInstance.implicits._
////    println("=== Test point")
////    val point = Point(0, Array(0.1, 0.2), 0, None)
////    assert(point.center.minDist(geometry.Point(Array(0.1, 0.2))) == 0)
////    val point2 = geometry.Point(Array(0.3, 0.4))
////    assert(point2.timeStamp == 0L)
////    println("=== Pass")
////    println("=== Test trajectory")
////    println(traj.mbr)
////    val traj2 = geometry.Trajectory(0L, 0L,
////      Array(geometry.Point(Array(0.1, 0.2), 1L), geometry.Point(Array(0.1, 0.3), 2L)),
////      Map("test" -> "trajectory"))
////    trajectory2Points(traj2).foreach(println(_))
////    println("=== Pass")
////  }
//}
