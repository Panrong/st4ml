package instances

object test extends App {

  val a = Array(
    Entry(Point(1, 1), Duration(10000L, 20000L))
  )

  val b = Event(entries = a, data = 1)
//  println(b)

//  val c = b.mapData(_.toString)
//  println(c.data)

//  val d = c.mapEntries(_ + Point(1,2), _.plusSeconds(1,1), _.toString)
//  println(d)
//  println(d.entries(0).temporal)

//  import GeometryImplicits.withExtraPointOps
//  val e = b.mapSpatial(_ + Point(1,2))
//
//  print(e)

//  val f = Event(Array(Entry(Point(1, 1), Duration(10000L, 20000L))), 1)
//  val g = Event(Array(Entry(Point(1, 1), Duration(10000L, 20000L))), 1)
//  println(f == g)
//  println(f.hashCode())
//  println(g.hashCode())
//
//  case class Person[T](name: T)
//  case class NestedPerson[S, T](p: Array[Person[S]], age: T = None)
//  val p1 = Person("A")
//  val p2 = Person("A")
//  println(p1 == p2)
//  println(p1.hashCode())
//  println(p2.hashCode())
//  val p3 = NestedPerson(Array(p1, p2), 2)
//  val p4 = NestedPerson(Array(p1, p2), 2)
//  println(p3 == p4)
//  println(p3.hashCode())
//  println(p4.hashCode())

//  case class Person[T](name: String, data: Option[T])
//
//  val pA = Person("A", None)
//  val pB = Person("B", Option(5))
//  val pC = Person("C", Option(Map(1->2, 2->3)))
//  println(pB.data.isEmpty)
//  println(pC.data.getClass)

  val m = Event(entries = a, data = None)
  println(m.toString)


}
