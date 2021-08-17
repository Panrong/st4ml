package instance

import instances._
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
      val pointArr: Array[Point] = Array(
        Point(0, 0),
        Point(0.5, 0.5),
        Point(1, 1),
        Point(2.5, 2.5),
        Point(4, 4)
      )
      val durArr: Array[Duration] = Array(
        Duration(0, 0),     // bin0
        Duration(0, 50),    // bin0
        Duration(50, 100),  // bin0,1
        Duration(30, 500),  // bin0,1,2,3
        Duration(200, 301)  // bin1,2,3
      )
      val eventArr: Array[eventType] =
        pointArr.zip(durArr).map(x => Event(x._1, x._2))

      val eventRasterSpatial = emptyRaster.attachInstance(eventArr, how="spatial")
      val eventRasterTemporal = emptyRaster.attachInstance(eventArr, how="temporal")
      val eventRasterBoth = emptyRaster.attachInstance(eventArr, how="both")
      val eventRasterEither = emptyRaster.attachInstance(eventArr, how="either")

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




  }

}
