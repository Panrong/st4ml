//import geometry._
//
//import scala.reflect.ClassTag
//
//object geoTest extends App {
//  override def main(args: Array[String]): Unit = {
//    implicit def tuple2Array[T: ClassTag](x: (T, T)): Array[T] = Array(x._1, x._2)
//
//    implicit def tuple4Array[T: ClassTag](x: (T, T, T, T)): Array[T] = Array(x._1, x._2, x._3, x._4)
//
//    var point1 = Point((2.0, 3.0))
//    println(s"created point $point1")
//    println(s"  dimensions ${point1.dimensions}")
//    println(s"  test + (0,1) : ${point1 + Point((0.0, 1.0))}")
//    println(s"  test - (0,1) : ${point1 - Point((0.0, 1.0))}")
//    println(s"  test * (0,1) : ${point1 * Point((0.0, 1.0))}")
//    println(s"  test * 2 : ${point1 * 2}")
//    println(s"  test normSquare : ${point1.normSquare}")
//    println(s"  test mbr : ${point1.mbr}")
//    println(s"  test center : ${point1.center()}")
//    println(s"  test inside (0,0,10,10) : ${point1.inside(Rectangle((0.0, 0.0, 10.0, 10.0)))}")
//    point1 = point1.assignID(11)
//    point1 = point1.assignTimeStamp(22)
//    println(s"  test assignID : ${point1.id}")
//    println(s"  test assignTimeStamp : ${point1.timeStamp}")
//    val point2 = Point((4.0, 3.0), 24)
//    println(s"created point $point2")
//    println(s"  test == : ${point1 == point2}")
//    println(s"  test <= : ${point1 <= point2}")
//    println(s"  test calSpeed : ${point1.calSpeed(point2)}")
//    println(s"  test geoDistance : ${point1.geoDistance(point2)}")
//
//    var rectangle1 = Rectangle((1.0, 2.0, 3.0, 4.0))
//    println(s"created rectangle $rectangle1")
//    println(s"  test area : ${rectangle1.area}")
//    println(s"  test center : ${rectangle1.center()}")
//    rectangle1 = rectangle1.assignID(11)
//    println(s"  test assignID : ${rectangle1.id}")
//    println(s" test overlap :  ${rectangle1.overlap(Rectangle((1.0, 1.0, 3.0, 3.0)))}")
//
//    var line1 = Line(Point((1.0, 2.0)), Point((3.0, 4.0)))
//    println(s"created line $line1")
//    println(s"  test midPoint : ${line1.midPoint}")
//    println(s"  test midPointDist to $point1: ${line1.midPointDistance(point2)._1}")
//    println(s"  test coordinates : ${line1.coordinates.deep}")
//    println(s"  test center : ${line1.center()}")
//    println(s"  test inside (0,0,10,10) : ${line1.inside(Rectangle((0.0, 0.0, 10.0, 10.0)))}")
//    println(s"  test projection distance to $point2 :${line1.projectionDistance(point1)._1}")
//    println(s"  test length : ${line1.length}")
//    println(s"  test mbr : ${line1.mbr}")
//    println(s"  test crosses : ${line1.crosses(Line(Point((3.0, 4.0)), Point((1.0, 2.0))))}")
//
//    println(s"  test intersection: point $point1 and rectangle $rectangle1 : ${point1.intersect(rectangle1)}")
//    println(s"  test intersection: point $point1 and line $line1 : ${point1.intersect(line1)}")
//    println(s"  test intersection: line $line1 and rectangle $rectangle1 : ${line1.intersect(rectangle1)}")
//  }
//}
