package instance

import instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SpatialMapSpec extends AnyFunSpec with Matchers{
  val extendArr = Array(
    Extent(0, 0, 1, 1),
    Extent(0, 1, 1, 2),
    Extent(1, 1, 2, 2),
    Extent(1, 0, 2, 1)
  )

  it("should create an Empty SpatialMap") {
    SpatialMap.empty[Point](extendArr)
  }

}
