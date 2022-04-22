package instance

import st4ml.instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RasterSpec extends AnyFunSpec with Matchers {
  describe("Raster") {
    val sBins: Array[Extent] = Array(
      Extent(0, 0, 1, 1),
      Extent(1, 0, 2, 1),
      Extent(1, 1, 2, 2),
      Extent(0, 1, 1, 2)
    )
    val tBins: Array[Duration] = Array(
      Duration(0, 100),
      Duration(100, 200),
      Duration(200, 300),
      Duration(300, 400),
    )
    val pointArr: Array[Point] = Array(
      Point(0, 0),
      Point(0.5, 0.5),
      Point(1, 1),
      Point(2.5, 2.5),
      Point(4, 4)
    )
    val durArr: Array[Duration] = Array(
      Duration(0, 0), // bin0
      Duration(0, 50), // bin0
      Duration(50, 100), // bin0,1
      Duration(30, 500), // bin0,1,2,3
      Duration(200, 301) // bin1,2,3
    )

    val trajArr: Array[Trajectory[None.type, None.type]] = Array(
      Trajectory(Array(Point(0, 0), Point(1, 1), Point(0.2, 0.2)), Array(Duration(0), Duration(52), Duration(143))),
      Trajectory(Array(Point(1, 1), Point(3, 3.2), Point(2.4, 3.7)), Array(Duration(83), Duration(115), Duration(463))),
      Trajectory(Array(Point(0.2, 0.4), Point(1.1, 1.3), Point(3.2, 2.4)), Array(Duration(19), Duration(83), Duration(243))),
      Trajectory(Array(Point(1.5, 1.7), Point(1.6, 3.2), Point(2.7, 0.2)), Array(Duration(153), Duration(252), Duration(343))),
    )

    it("can create an Empty Raster") {
      val emptyRaster = Raster.empty[Point](sBins, tBins)
      emptyRaster.data shouldBe None
      emptyRaster.entries.map(_.spatial) shouldBe sBins.map(_.toPolygon)
      emptyRaster.entries.map(_.temporal) shouldBe tBins
      emptyRaster.entries.map(_.value) shouldBe Array(Array.empty[Point], Array.empty[Point], Array.empty[Point], Array.empty[Point])
    }

    it("can allocate Instance objects based on instance's extent and duration") {
      type eventType = Event[Point, None.type, None.type]
      val emptyRaster = Raster.empty[eventType](sBins, tBins)
      val eventArr: Array[eventType] =
        pointArr.zip(durArr).map(x => Event(x._1, x._2))

      val eventRasterSpatial = emptyRaster.attachInstance(eventArr, how = "spatial")
      val eventRasterTemporal = emptyRaster.attachInstance(eventArr, how = "temporal")
      val eventRasterBoth = emptyRaster.attachInstance(eventArr, how = "both")
      val eventRasterEither = emptyRaster.attachInstance(eventArr, how = "either")

      eventRasterSpatial.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2)),
        Array(eventArr(2)),
        Array(eventArr(2)),
        Array(eventArr(2))
      )

      eventRasterTemporal.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2), eventArr(3)),
        Array(eventArr(2), eventArr(3), eventArr(4)),
        Array(eventArr(3), eventArr(4)),
        Array(eventArr(3), eventArr(4))
      )

      eventRasterBoth.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2)),
        Array(eventArr(2)),
        Array.empty[eventType],
        Array.empty[eventType]
      )

      eventRasterEither.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2), eventArr(3)),
        Array(eventArr(2), eventArr(3), eventArr(4)),
        Array(eventArr(2), eventArr(3), eventArr(4)),
        Array(eventArr(2), eventArr(3), eventArr(4)),
      )

    }

    it("can merge Raster of the same type") {
      type eventType = Event[Point, None.type, None.type]
      val emptyRaster = Raster.empty[eventType](sBins, tBins)
      val eventArr: Array[eventType] =
        pointArr.zip(durArr).map(x => Event(x._1, x._2))
      val eventRasterBoth = emptyRaster.attachInstance(eventArr, how = "both")
      val rasterMergerd = eventRasterBoth.merge(eventRasterBoth)
      rasterMergerd.entries.map(_.value) shouldBe Array(
        Array(eventArr(0), eventArr(1), eventArr(2), eventArr(0), eventArr(1), eventArr(2)),
        Array(eventArr(2), eventArr(2)),
        Array.empty[eventType],
        Array.empty[eventType]
      )
    }

    it("can allocate Instance objects with Rtree") {
      /** check if allocateInstance and allocateInstanceRTree result the same */
      val pointInstanceArr = pointArr.map(p => Event(p, Duration(0)))
      val pointRaster = Raster.empty[Event[Point, None.type, None.type]](sBins, tBins)
        .attachInstanceRTree(pointInstanceArr)
      val pointRaster2 = Raster.empty[Event[Point, None.type, None.type]](sBins, tBins)
        .attachInstance(pointInstanceArr)
      pointRaster.entries.map(_.value) shouldBe pointRaster2.entries.map(_.value)

      val trajRaster = Raster.empty[Trajectory[None.type, None.type]](sBins, tBins)
        .attachInstanceRTree(trajArr)
      val trajRaster2 = Raster.empty[Trajectory[None.type, None.type]](sBins, tBins)
        .attachInstance(trajArr)
      trajRaster.entries.map(_.value) shouldBe trajRaster2.entries.map(_.value)
    }

  }

}
