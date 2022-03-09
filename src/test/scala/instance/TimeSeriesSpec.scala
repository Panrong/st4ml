package instance

import org.apache.spark.sql.SparkSession
import st4ml.instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import Utils._

class TimeSeriesSpec extends AnyFunSpec with Matchers {
  describe("TimeSeries") {
    val durbinsDisjoint: Array[Duration] = Array(
      Duration(0L, 100L),
      Duration(100L, 200L),
      Duration(200L, 300L))
    val durbinsOverlapping: Array[Duration] = Array(
      Duration(0L, 100L),
      Duration(50L, 200L),
      Duration(150L, 300L))

    val pointArr: Array[Point] = Array(
      Point(0, 0),
      Point(1, 1),
      Point(2, 2),
      Point(3, 3),
      Point(4, 4))
    val lineStringArr: Array[LineString] = Array(
      LineString(Point(0, 0), Point(-1, -1)),
      LineString(Point(1, 1), Point(-1, -1)),
      LineString(Point(2, 2), Point(-1, -1)),
      LineString(Point(3, 3), Point(-1, -1)),
      LineString(Point(4, 4), Point(-1, -1)),
    )
    val polygonArr: Array[Polygon] = Array(
      Polygon(Point(0, 0), Point(-1, -1), Point(-2, -1), Point(0, 0)),
      Polygon(Point(1, 1), Point(-1, -1), Point(-2, -1), Point(1, 1)),
      Polygon(Point(2, 2), Point(-1, -1), Point(-2, -1), Point(2, 2)),
      Polygon(Point(3, 3), Point(-1, -1), Point(-2, -1), Point(3, 3)),
      Polygon(Point(4, 4), Point(-1, -1), Point(-2, -1), Point(4, 4)),
    )
    val eventArr: Array[Event[Point, None.type, None.type]] = pointArr.map(x => Event(x, Duration.empty))
    val trajArr: Array[Trajectory[None.type, None.type]] = lineStringArr.map(x => Trajectory(
      Array(x.getPointN(0), x.getPointN(1)),
      Array(Duration.empty, Duration.empty)
    ))

    val timestampArr: Array[Long] = Array(0L, 50L, 100L, 150L, 250L)
    val durationArr: Array[Duration] = Array(
      Duration(0L, 50L),
      Duration(0L, 150L),
      Duration(100L, 201L),
      Duration(201L, 250L),
      Duration(350L, 351L)
    )


    it("can create an Empty TimeSeries") {
      val emptyTs = TimeSeries.empty[Point](durbinsDisjoint)
      emptyTs.data shouldBe None
      emptyTs.entries.map(_.spatial) shouldBe Array(Polygon.empty, Polygon.empty, Polygon.empty)
      emptyTs.entries.map(_.temporal) shouldBe durbinsDisjoint
      emptyTs.entries.map(_.value) shouldBe Array(Array.empty[Point], Array.empty[Point], Array.empty[Point])
    }

    it("can check if temporals are disjoint") {
      val emptyTsDisjoint = TimeSeries.empty[Point](durbinsDisjoint)
      emptyTsDisjoint.isTemporalDisjoint shouldBe true

      val emptyTsOverlapping = TimeSeries.empty[Point](durbinsOverlapping)
      emptyTsOverlapping.isTemporalDisjoint shouldBe false
    }

    it("can allocate Geometry objects based on the input timestamp") {

      val pointTs = TimeSeries.empty[Point](durbinsDisjoint).attachGeometry(pointArr, timestampArr)
      val lineStringTs = TimeSeries.empty[LineString](durbinsDisjoint).attachGeometry(lineStringArr, timestampArr)
      val polygonTs = TimeSeries.empty[Polygon](durbinsDisjoint).attachGeometry(polygonArr, timestampArr)

      pointTs.entries.map(_.value) shouldBe Array(
        Array(pointArr(0), pointArr(1), pointArr(2)),
        Array(pointArr(3)),
        Array(pointArr(4))
      )

      lineStringTs.entries.map(_.value) shouldBe Array(
        Array(lineStringArr(0), lineStringArr(1), lineStringArr(2)),
        Array(lineStringArr(3)),
        Array(lineStringArr(4))
      )

      polygonTs.entries.map(_.value) shouldBe Array(
        Array(polygonArr(0), polygonArr(1), polygonArr(2)),
        Array(polygonArr(3)),
        Array(polygonArr(4))
      )
    }

    //  it("won't compile when attach mismatched Geometry objects") {
    //    val mismatchedTs = TimeSeries.empty[LineString](durBins).attachGeometry(pointArr, timeArr)
    //  }

    it("can allocate Instance objects based on the input timestamp") {
      val eventTs = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr, timestampArr)
      val trajTs = TimeSeries.empty[Trajectory[None.type, None.type]](durbinsDisjoint).attachInstance(trajArr, timestampArr)

      eventTs.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2)),
        Array(eventArr(3)),
        Array(eventArr(4))
      )

      trajTs.entries.map(_.value) shouldBe Array(
        Array(trajArr(0), trajArr(1), trajArr(2)),
        Array(trajArr(3)),
        Array(trajArr(4))
      )
    }

    //  it("won't compile when attach mismatched Instance objects") {
    //    val mismatchedTs = TimeSeries.empty[Event[_,_,_]](durBins).attachGeometry(eventArr, timeArr)
    //  }

    it("can allocate Geometry/Instance based on their duration") {
      val trajTs = TimeSeries.empty[Trajectory[None.type, None.type]](durbinsDisjoint)
        .attachInstance(trajArr, durationArr)
      trajTs.entries.map(_.value) shouldBe Array(
        Array(trajArr(0), trajArr(1), trajArr(2)),
        Array(trajArr(1), trajArr(2)),
        Array(trajArr(2), trajArr(3))
      )

      val trajTs2 = TimeSeries.empty[Trajectory[None.type, None.type]](durbinsDisjoint)
        .attachInstance(trajArr)

      trajTs2.entries.map(_.value) shouldBe Array(
        Array(),
        Array(),
        Array()
      )


    }

    it("can merge TimeSeries of same type") {
      val eventTs1 = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr, timestampArr)

      val eventArr2 = pointArr.map(x => Event(x, Duration(1L)))
      val eventTs2 = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr2, timestampArr)


      val eventTsMerged = eventTs1.merge(eventTs2)

      eventTsMerged.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2), eventArr2(0), eventArr2(1), eventArr2(2)),
        Array(eventArr(3), eventArr2(3)),
        Array(eventArr(4), eventArr2(4))
      )
    }

    it("can split a TimeSeries at certain timestamp") {
      val eventTs = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr, timestampArr)

      val parts = eventTs.split(150L)
      parts._1.entries.map(_.temporal) shouldBe Array(durbinsDisjoint(0), durbinsDisjoint(1))
      parts._2.entries.map(_.temporal) shouldBe Array(durbinsDisjoint(2))


      val parts2 = eventTs.split(100L)
      parts2._1.entries.map(_.temporal) shouldBe Array(durbinsDisjoint(0))
      parts2._2.entries.map(_.temporal) shouldBe Array(durbinsDisjoint(1), durbinsDisjoint(2))

      the[IllegalArgumentException] thrownBy eventTs.split(250L)
      the[IllegalArgumentException] thrownBy eventTs.split(75L)
    }

    it("can select sub TimeSeries from a TimeSeries based on Array[Duration]") {
      val eventTs = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr, timestampArr)

      val targetDurBins = Array(durbinsDisjoint(0), durbinsDisjoint(2))
      eventTs.select(targetDurBins).entries.map(_.temporal) shouldBe targetDurBins
    }

    it("can append two TimeSeries of the same type") {
      val eventTs1 = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr, timestampArr)
      val eventTs2 = TimeSeries.empty[Event[Point, None.type, None.type]](durbinsDisjoint).attachInstance(eventArr, timestampArr)

      val appendedTs = eventTs1.append(eventTs2)
      appendedTs.entries.map(_.value) shouldBe
        eventTs1.entries.map(_.value) ++ eventTs1.entries.map(_.value)
    }

    it("can allocate Instance objects with Rtree") {
      /** check if allocateInstance and allocateInstanceRTree result the same */
      val pointInstanceArr = pointArr.map(p => Event(p, Duration(0)))
      val pointTs = TimeSeries.empty[Event[Point, None.type, None.type]](durationArr)
        .attachInstanceRTree(pointInstanceArr)
      val pointTs2 = TimeSeries.empty[Event[Point, None.type, None.type]](durationArr)
        .attachInstance(pointInstanceArr)

      pointTs.entries.map(_.value) shouldBe pointTs2.entries.map(_.value)
    }


    //    it("ts rdd functions work") {
//      val sc = SparkSession.builder().master("local[2]")
//        .appName("test").getOrCreate().sparkContext
//      val ts1 = TimeSeries(Array(Duration(0), Duration(1), Duration(2)), Array(Array(Point(0, 0)), Array(Point(1, 1)), Array(Point(2, 2))))
//      val ts2 = TimeSeries(Array(Duration(0), Duration(1), Duration(2)), Array(Array(Point(10, 10)), Array(Point(11, 11)), Array(Point(12, 12))))
//      val tsRDD = sc.parallelize(Array(ts1, ts2))
//
//      def f(v: Point, t: Duration): Point = {
//        Point(v.getX + t.start, v.getY + t.end)
//      }
//
//      val tsRDD2 = tsRDD.mapValuePlus2(f)
//      val a = tsRDD2.collect()
//      println(a.flatMap(x => x.entries.map(_.value)).deep)
//    }

  }
}
