package instance

import instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SpatialMapSpec extends AnyFunSpec with Matchers{
  describe("SpatialMap") {
    val extendArrOverlapping: Array[Extent] = Array(
      Extent(0, 0, 1, 1),
      Extent(1, 0, 2, 1),
      Extent(1, 1, 2, 2),
      Extent(0, 1, 1, 2)
    )

    val extendArrDisjoint: Array[Extent] = Array(
      Extent(0, 0, 1, 1),
      Extent(1, 0, 2, 1).translateBy(1, 0),
      Extent(1, 1, 2, 2).translateBy(1, 1),
      Extent(0, 1, 1, 2).translateBy(0, 1)
    )
    val pointArr: Array[Point] = Array(
      Point(0, 0),
      Point(0.5, 0.5),
      Point(1, 1),
      Point(2.5, 2.5),
      Point(4, 4))
    val lineStringArr: Array[LineString] = Array(
      LineString(Point(0, 0), Point(-1, -1)),
      LineString(Point(1, 1), Point(2, 0), Point(3, -1)),
      LineString(Point(1, 1), Point(3, -1))
    )
    val polygonArr: Array[Polygon] = Array(
      Polygon(Point(0, 0), Point(-1, -1), Point(-2, -1), Point(0, 0)),
      Polygon(Point(1, 1), Point(2, 1), Point(2, 2), Point(1, 2), Point(1, 1)),
      Polygon(Point(-1, -1), Point(10, -1), Point(10, 10), Point(-1, 10), Point(-1, -1)),
    )

    val eventArr: Array[Event[Point, None.type, None.type]] = pointArr.map(x => Event(x, Duration.empty))
    val trajArr: Array[Trajectory[None.type, None.type]] = lineStringArr.map(x => Trajectory(
      Array(x.getPointN(0), x.getPointN(1)),
      Array(Duration.empty, Duration.empty)
    ))

    it("can create an Empty SpatialMap") {
      SpatialMap.empty[Point](extendArrOverlapping)
    }

    it("can check if spatials are disjoint") {
      val emptySmOverlapping = SpatialMap.empty[Point](extendArrOverlapping)
      emptySmOverlapping.isSpatialDisjoint shouldBe true

      val emptySmDisjoint = SpatialMap.empty[Point](extendArrDisjoint)
      emptySmDisjoint.isSpatialDisjoint shouldBe true
    }

    it("can allocate Geometry objects") {
      val pointSm = SpatialMap.empty[Point](extendArrDisjoint).attachGeometry(pointArr)
      val lineStringSm = SpatialMap.empty[LineString](extendArrDisjoint).attachGeometry(lineStringArr)
      val polygonSm = SpatialMap.empty[Polygon](extendArrDisjoint).attachGeometry(polygonArr)

      pointSm.entries.map(_.value) shouldBe Array(
        Array(pointArr(0), pointArr(1), pointArr(2)),
        Array.empty[Point],
        Array(pointArr(3)),
        Array.empty[Point]
      )

      lineStringSm.entries.map(_.value) shouldBe Array(
        Array(lineStringArr(0), lineStringArr(1), lineStringArr(2)),
        Array(lineStringArr(1), lineStringArr(2)),
        Array.empty[LineString],
        Array.empty[LineString]
      )

      polygonSm.entries.map(_.value) shouldBe Array(
        Array(polygonArr(0), polygonArr(1), polygonArr(2)),
        Array(polygonArr(1), polygonArr(2)),
        Array(polygonArr(1), polygonArr(2)),
        Array(polygonArr(1), polygonArr(2)),
      )
    }

    //    it("won't compile when attach mismatched Geometry objects") {
    //      val mismatchedSm = SpatialMap.empty[LineString](extendArrDisjoint)
    //        .attachGeometry(pointArr)
    //    }

    it("can allocate Instance objects based on the input geometry") {
      val eventGeom = eventArr.flatMap(_.entries.map(_.spatial))
      val eventSm = SpatialMap.empty[Event[Point, None.type, None.type]](extendArrDisjoint)
        .attachInstance(eventArr, eventGeom)

      val trajGeom = trajArr.map(_.entries.map(_.spatial)).map(LineString(_))
      val trajSm = SpatialMap.empty[Trajectory[None.type, None.type]](extendArrDisjoint)
        .attachInstance(trajArr, trajGeom)

      eventSm.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2)),
        Array.empty[Point],
        Array(eventArr(3)),
        Array.empty[Point]
      )

      trajSm.entries.map(_.value) shouldBe Array(
        Array(trajArr(0), trajArr(1), trajArr(2)),
        Array(trajArr(1), trajArr(2)),
        Array.empty[LineString],
        Array.empty[LineString]
      )
    }

    it("can allocate Instance objects based on instance's spatial") {
      val eventSm = SpatialMap.empty[Event[Point, None.type, None.type]](extendArrDisjoint)
        .attachInstance(eventArr)
      val trajSm = SpatialMap.empty[Trajectory[None.type, None.type]](extendArrDisjoint)
        .attachInstance(trajArr)

      eventSm.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2)),
        Array.empty[Point],
        Array(eventArr(3)),
        Array.empty[Point]
      )

      trajSm.entries.map(_.value) shouldBe Array(
        Array(trajArr(0), trajArr(1), trajArr(2)),
        Array(trajArr(1), trajArr(2)),
        Array.empty[LineString],
        Array.empty[LineString]
      )
    }

    //    it("won't compile when attach mismatched Instance objects") {
    //      val mismatchedSm = SpatialMap.empty[Event[_,_,_]](extendArrDisjoint)
    //        .attachGeometry(eventArr)
    //    }

    it("can handle empty attachment") {
      val eventEmptySm = SpatialMap.empty[Event[Point, None.type, None.type]](extendArrDisjoint)

      val eventSm = eventEmptySm.attachInstance(Array.empty[Event[Point, None.type, None.type]])
      eventSm shouldBe eventEmptySm

    }

    it("can merge SpatialMap of same type") {
      val eventSm = SpatialMap.empty[Event[Point, None.type, None.type]](extendArrDisjoint)
        .attachInstance(eventArr)

      val eventSmMergerd = eventSm.merge(eventSm)
      eventSmMergerd.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2), eventArr(0), eventArr(1), eventArr(2)),
        Array.empty[Point],
        Array(eventArr(3), eventArr(3)),
        Array.empty[Point]
      )
    }



  }

}
