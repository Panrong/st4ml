package instances

object test extends App {

  val a = Array(
    Entry(Point(1, 1), Duration(10000L, 20000L), 1)
  )

  val b = Event(entries = a, data = 1)
//  println(b)

//  val c = b.mapData(_.toString)
//  println(c.data)

//  val d = c.mapEntries(_ + Point(1,2), _.plusSeconds(1,1), _.toString)
//  println(d)
//  println(d.entries(0).temporal)

  import GeometryImplicits.withExtraPointOps
  val e = b.mapSpatial(_ + Point(1,2))
  print(e)

}
