package geometry

object LineStringTest extends App {
  // a point near the following ls
  val p0 = Point(-8.589238, 41.223076)

  // edge 702048278-702048270, 198.669 meters
  val p1 = Point(-8.5896452, 41.2237259)
  val p2 = Point(-8.5897313, 41.2233232)
  val p3 = Point(-8.5898836, 41.2224952)
  val p4 = Point(-8.589930900000001, 41.2222241)
  val p5 = Point(-8.5900175, 41.2219626)

  val line1 = Line(p1, p2)
  val ls1 = LineString(Array(p1, p2, p3, p4, p5))

  println(s"Test function greatCircleDistance with input: $p1, $p2")
  println("-- Expected result: about 45.35 meters")
  println("-- Actual result: " + Distances.greatCircleDistance(p1, p2))
  println("-------------------------------------")
  println(s"Test function point2LineMiddlePointDistance with input: $p0, $line1")
  println("-- Expected result: (About 62.50 meters, Point(-8.58968825,41.22352455))")
  println("-- Actual result: " + line1.midPointDistance(p0))
  println("-------------------------------------")
  println(s"Test function point2LineStringMiddlePointDistance with input: $p0, $ls1")
  println(s"-- Expected result: (About 51.11 meters, Point(${(p2.lon+p3.lon)/2}, ${(p2.lat+p3.lat)/2}))")
  println("-- Actual result: " + ls1.midPointDistance(p0))
  println("-------------------------------------")


  val p6 = Point(0, 0)
  val p7 = Point(1, 1)
  val p8 = Point(2, 1)

  val p9 = Point(0, 2)
  val p10 = Point(0, 1)
  val line2 = Line(p6, p7)
  val line3 = Line(p7, p8)
  val ls2 = LineString(Array(p6, p7, p8))

  println(s"Test function point2LineProjectionDistance with input: $p9, $line2")
  println("-- Expected result: (-, Point(1,1))")
  println("-- Actual result: " + line2.projectionDistance(p9))
  println("-------------------------------------")
  println(s"Test function point2LineProjectionDistance with input: $p9, $line3")
  println("-- Expected result: (-, Point(1,1))")
  println("-- Actual result: " + line3.projectionDistance(p9))
  println("-------------------------------------")
  println(s"Test function point2LineProjectionDistance with input: $p10, $line2")
  println("-- Expected result: (-, Point(0.5,0.5))")
  println("-- Actual result: " + line2.projectionDistance(p10))
  println("-------------------------------------")

  println(s"Test function point2LineProjectionDistance with input: $p10, $ls2")
  println("-- Expected result: (-, Point(0.5,0.5))")
  println("-- Actual result: " + ls2.projectionDistance(p10))
  println("-------------------------------------")
  println(s"Test function point2LineProjectionDistance with input: $p0, $ls1")
  println("-- Expected result: (Around 45, Point(-8.5897313~-8.5898836, 41.2233232~41.2224952)")
  println("-- Actual result: " + ls1.projectionDistance(p0))
  println("-------------------------------------")
}
