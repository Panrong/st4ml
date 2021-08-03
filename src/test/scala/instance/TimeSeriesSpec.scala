package instance

import instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TimeSeriesSpec extends AnyFunSpec with Matchers{
  val durBins: Array[Duration] = Array(
    Duration(0L, 100L),
    Duration(100L, 200L),
    Duration(200L, 300L))
  val pointArr: Array[Point] = Array(
    Point(0, 0),
    Point(1, 1),
    Point(2, 2),
    Point(3, 3),
    Point(4, 4))
  val lineStringArr: Array[LineString] = Array(
    LineString(Point(0,0), Point(-1,-1)),
    LineString(Point(1,1), Point(-1,-1)),
    LineString(Point(2,2), Point(-1,-1)),
    LineString(Point(3,3), Point(-1,-1)),
    LineString(Point(4,4), Point(-1,-1)),
  )
  val polygonArr: Array[Polygon] = Array(
    Polygon(Point(0,0), Point(-1,-1), Point(-2,-1), Point(0,0)),
    Polygon(Point(1,1), Point(-1,-1), Point(-2,-1), Point(1,1)),
    Polygon(Point(2,2), Point(-1,-1), Point(-2,-1), Point(2,2)),
    Polygon(Point(3,3), Point(-1,-1), Point(-2,-1),  Point(3,3)),
    Polygon(Point(4,4), Point(-1,-1), Point(-2,-1), Point(4,4)),
  )
  val eventArr: Array[Event[Point, None.type, None.type]] = pointArr.map(x => Event(x, Duration.empty))
  val trajArr: Array[Trajectory[None.type, None.type]] = lineStringArr.map(x => Trajectory(
    Array(x.getPointN(0), x.getPointN(1)),
    Array(Duration.empty, Duration.empty)
  ))

  val timeArr: Array[Long] = Array(0L, 50L, 100L, 150L, 250L)


  it("should create an Empty TimeSeries") {
    val emptyTs = TimeSeries.empty[Point](durBins)
    emptyTs.data shouldBe None
    emptyTs.entries.map(_.spatial) shouldBe Array(Polygon.empty, Polygon.empty, Polygon.empty)
    emptyTs.entries.map(_.temporal) shouldBe durBins
    emptyTs.entries.map(_.value) shouldBe Array(Array.empty[Point], Array.empty[Point], Array.empty[Point])
  }

  it("can allocate Geometry objects based on their timestamp") {

    val pointTs = TimeSeries.empty[Point](durBins).attachGeometry(pointArr, timeArr)
    val lineStringTs = TimeSeries.empty[LineString](durBins).attachGeometry(lineStringArr, timeArr)
    val polygonTs = TimeSeries.empty[Polygon](durBins).attachGeometry(polygonArr, timeArr)

    pointTs.entries.map(_.value) shouldBe Array(
      Array(pointArr(0), pointArr(1)),
      Array(pointArr(2), pointArr(3)),
      Array(pointArr(4))
    )

    lineStringTs.entries.map(_.value) shouldBe Array(
      Array(lineStringArr(0), lineStringArr(1)),
      Array(lineStringArr(2), lineStringArr(3)),
      Array(lineStringArr(4))
    )

    polygonTs.entries.map(_.value) shouldBe Array(
      Array(polygonArr(0), polygonArr(1)),
      Array(polygonArr(2), polygonArr(3)),
      Array(polygonArr(4))
    )
  }

//  it("won't compile when attach mismatched Geometry objects") {
//    val mismatchedTs = TimeSeries.empty[LineString](durBins).attachGeometry(pointArr, timeArr)
//  }

  it("can allocate Instance objects based on their timestamp") {
    val eventTs = TimeSeries.empty[Event[Point, None.type, None.type]](durBins).attachInstance(eventArr, timeArr)
    val trajTs = TimeSeries.empty[Trajectory[None.type, None.type]](durBins).attachInstance(trajArr, timeArr)
    
    eventTs.entries.map(_.value) shouldBe Array(
      Array(eventArr(0), eventArr(1)),
      Array(eventArr(2), eventArr(3)),
      Array(eventArr(4))
    )

    trajTs.entries.map(_.value) shouldBe Array(
      Array(trajArr(0), trajArr(1)),
      Array(trajArr(2), trajArr(3)),
      Array(trajArr(4))
    )
  }

//  it("won't compile when attach mismatched Instance objects") {
//    val mismatchedTs = TimeSeries.empty[Event[_,_,_]](durBins).attachGeometry(eventArr, timeArr)
//  }

  it("can merge TimeSeries of same type") {
    val eventTs1 = TimeSeries.empty[Event[Point, None.type, None.type]](durBins).attachInstance(eventArr, timeArr)

    val eventArr2 = pointArr.map(x => Event(x, Duration(1L)))
    val eventTs2 = TimeSeries.empty[Event[Point, None.type, None.type]](durBins).attachInstance(eventArr2, timeArr)


    val eventTsMerged = eventTs1.merge(eventTs2)

    eventTsMerged.entries.map(_.value) shouldBe Array(
      Array(eventArr(0), eventArr(1), eventArr2(0), eventArr2(1)),
      Array(eventArr(2), eventArr(3), eventArr2(2), eventArr2(3)),
      Array(eventArr(4), eventArr2(4))
    )
  }

  it("can split a TimeSeries at certain timestamp") {
    val eventTs = TimeSeries.empty[Event[Point, None.type, None.type]](durBins).attachInstance(eventArr, timeArr)

    val parts = eventTs.split(150L)
    parts._1.entries.map(_.temporal) shouldBe Array(durBins(0), durBins(1))
    parts._2.entries.map(_.temporal) shouldBe Array(durBins(2))


    val parts2 = eventTs.split(100L)
    parts2._1.entries.map(_.temporal) shouldBe Array(durBins(0))
    parts2._2.entries.map(_.temporal) shouldBe Array(durBins(1), durBins(2))

    the [IllegalArgumentException] thrownBy eventTs.split(250L)
    the [IllegalArgumentException] thrownBy eventTs.split(75L)
  }

  it("can select sub TimeSeries from a TimeSeries based on Array[Duration]") {
    val eventTs = TimeSeries.empty[Event[Point, None.type, None.type]](durBins).attachInstance(eventArr, timeArr)

    val targetDurBins = Array(durBins(0), durBins(2))
    eventTs.select(targetDurBins).entries.map(_.temporal) shouldBe targetDurBins
  }


}
