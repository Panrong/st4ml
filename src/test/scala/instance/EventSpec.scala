package instance

import instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class EventSpec extends AnyFunSpec with Matchers {
  describe("Event") {
    val p = Point(123.321, -0.4343434)
    val ls = LineString(Point(0, 0), Point(2, 2))
    val poly = Polygon(LineString(Point(0, 0), Point(0, 1), Point(1, 1), Point(1, 0), Point(0, 0)))
    val t = Duration(1626154661L, 1626154683L)

    it("can be initialized without the second argument") {
      Event(Array(Entry(p, t, None)))
      Event(Array(Entry(ls, t, None)))
      Event(Array(Entry(poly, t, None)))
    }

    it("can be initialized with a Geometry and a Duration") {
      Event(p, t) shouldBe Event(Array(Entry(p, t, None)), None)
      Event(ls, t) shouldBe Event(Array(Entry(ls, t, None)), None)
      Event(poly, t) shouldBe Event(Array(Entry(poly, t, None)), None)
    }

    it("can convert to a geometry") {
      Event(p, t).toGeometry shouldBe p
      Event(ls, t).toGeometry shouldBe ls
      Event(poly, t).toGeometry shouldBe poly
    }
  }

}
