package instance

import instances.Distances.greatCircleDistance
import instances.{Duration, Extent, LineString, Point}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

class GeomSpec extends AnyFunSpec with Matchers {
  describe("Extent") {
    val e1 = Extent(0, 0, 2, 2)
    val e2 = Extent(2, 0, 4, 2) // shares an edge
    val e3 = Extent(1, 1, 3, 3) // intersects
    val e4 = Extent(2, 2, 3, 3) // shares a point
    val ls = LineString(Point(1, 1), Point(3, 3))
    it("correct intersects and contains") {
      e1.intersects(e2) shouldBe true
      e1.intersects(e3) shouldBe true
      e1.intersects(e4) shouldBe true
      e1.intersects(1, 1) shouldBe true
      e1.intersects(2, 2) shouldBe true
      e1.intersects(ls) shouldBe true
      e1.contains(1, 1) shouldBe true
      e1.contains(2, 2) shouldBe false
      e1.contains(Point(2, 2)) shouldBe false
      e1.contains(LineString(Point(1, 1), Point(2, 2))) shouldBe true
      e1.contains(Extent(0, 0, 1, 1)) shouldBe true
    }
    it("correct distance") {
      e1.distance(Extent(3, 3, 5, 5)) shouldBe math.sqrt(2)
      e1.distance(Extent(3, 0, 5, 5)) shouldBe 1
      e1.distance(e2) shouldBe 0
    }
    it("correct intersection") {
      e1.intersection(e2) shouldBe None
      e1.intersection(e3) shouldBe Some(Extent(1, 1, 2, 2))
    }
    it("correct compare") {
      e1.compare(e2) shouldBe -1
      e4.compare(e1) shouldBe 1
    }
    it("correct combine and expand") {
      e1.combine(e2) shouldBe Extent(0, 0, 4, 2)
      e1.combine(e3) shouldBe Extent(0, 0, 3, 3)
      e1.expandToInclude(3, 3) shouldBe Extent(0, 0, 3, 3)
      e1.expandBy(1, 2) shouldBe Extent(-1, -2, 3, 4)
    }
  }
  describe("Duration") {
    val t1 = Duration(0, 100)
    val t2 = Duration(100, 101)
    val t3 = Duration(50, 200)
    it("correct intersects") {
      t1 intersects t2 shouldBe true
      t1 intersects t3 shouldBe true
      t1 intersection t2 shouldBe None
      t1 intersection t3 shouldBe Some(Duration(50, 100))
      utils.TimeParsing.getHour(Duration(1638186168, 1638186168).start, TimeZone.getTimeZone("Asia/Shanghai")) shouldBe 19
      utils.TimeParsing.getHour(Duration(1638186168, 1638186168).start, TimeZone.getTimeZone("Asia/Tokyo")) shouldBe 20

    }
  }
}
