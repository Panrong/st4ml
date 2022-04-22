package instance

import st4ml.instances._
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

import java.util.TimeZone

class EntrySpec extends AnyFunSpec with Matchers {
  describe("Entry") {
    val p = Point(123.321, -0.4343434)
    val ls = LineString(Point(0, 0), Point(2, 2))
    val poly = Polygon(LineString(Point(0, 0), Point(0, 1), Point(1, 1), Point(1, 0), Point(0, 0)))
    val t = Duration(1626154661L, 1626154683L)

    it("can be initialized with a Geometry, a Duration and a generic type") {
      Entry(p, t, None)
      Entry(ls, t, None)
      Entry(poly, t, None)
    }

    it("can be initialized without the third argument") {
      Entry(p, t) shouldBe Entry(p, t, None)
      Entry(ls, t) shouldBe Entry(ls, t, None)
      Entry(poly, t) shouldBe Entry(poly, t, None)
    }

    it("should accept generic types as the input of the third argument") {
      Entry(p, t, 1)
      Entry(p, t, 2.0)
      Entry(p, t, Map("user1" -> 3.0, "user2" -> 1.0))
      Entry(p, t, Array(1, 2, 3))
    }

    it("can be initialized with tuple3") {
      Entry((p, t, None))
      Entry((ls, t, None))
      Entry((poly, t, None))
    }

    it("can be initialized with tuple2") {
      Entry((p, t))
      Entry((ls, t))
      Entry((poly, t))
    }

    it("duration construction with string") {
      Duration("2022-01-01 00:00:00") shouldBe Duration(1640966400)
      Duration("2022-01-01 00:00:00", timeZone = TimeZone.getTimeZone("GMT")) shouldBe Duration(1640995200)
      Duration(("2022-01-01 00:00:00", "2022-01-02 00:00:00"), format = "yyyy-MM-dd HH:mm:ss", timeZone =TimeZone.getDefault) shouldBe Duration(1640966400, 1641052800)
    }

  }

}
