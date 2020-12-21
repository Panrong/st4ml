package stInstance

import org.scalatest.funsuite.AnyFunSuite
import STInstance.{Trajectory, Point}

class implicitsSuite extends AnyFunSuite {
  val r = new scala.util.Random
  var coords = new Array[((Double, Double), Long)](0)
  val t = 0
  for (i <- 0 to 10) {
    coords = coords :+ ((r.nextDouble(), r.nextDouble()), (t + i).toLong)
  }
  val traj = Trajectory[(Double, Double), Null](
    id = 0,
    coords,
    property = None
  )
  test("STInstances: trajectory points conversion") {
    import STInstance.implicits._
    val points: Array[Point[Long]] = traj
    points.foreach(x => println(x))
  }
}
