package instance

import instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TimeSeriesSpec extends AnyFunSpec with Matchers{
  val durBins: Array[Duration] = Array(
    Duration(0L, 100L),
    Duration(100L, 200L),
    Duration(200L, 300L))


  it("should create an Empty TimeSeries") {
    val emptyTs = TimeSeries.empty(durBins)
    emptyTs.data shouldBe None
    emptyTs.entries.map(_.spatial) shouldBe Array(Polygon.empty, Polygon.empty, Polygon.empty)
    emptyTs.entries.map(_.temporal) shouldBe durBins
    emptyTs.entries.map(_.value) shouldBe Array(Array.empty[Geometry], Array.empty[Geometry], Array.empty[Geometry])
  }

  it("can allocate Geometry objects based on their timestamp") {
    val geomArr = Array(Point(0, 0), Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4))
    val timeArr = Array(0L, 50L, 100L, 150L, 250L)
    val ts = TimeSeries.empty(durBins).allocate(geomArr, timeArr)
    ts.entries.map(_.value) shouldBe Array(Array(Point(0,0), Point(1, 1)), Array(Point(2, 2), Point(3, 3)), Array(Point(4, 4)))
  }

}
