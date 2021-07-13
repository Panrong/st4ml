package instance

import instances._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TrajectorySpec extends AnyFunSpec with Matchers {
  describe("Trajectory") {
    val t = Duration(1626154661L, 1626154683L)
    val entryArr = Array(
      Entry(Point(0, 0), t),
      Entry(Point(1, 0), t),
      Entry(Point(1, 1), t),
      Entry(Point(0, 1), t))
    val pointArr = Array(
      Point(0, 0),
      Point(1, 0),
      Point(1, 1),
      Point(0, 1),
    )
    val durationArr = Array(t, t, t, t)
    val trajectory = Trajectory(entryArr, None)

    it("can be initialized with the data") {
      Trajectory(entryArr)
    }

    it("can be initialized with Array[Tuple3]") {
      val traj = Trajectory(
        Array(
          (Point(0, 0), t, None),
          (Point(1, 0), t, None),
          (Point(1, 1), t, None),
          (Point(0, 1), t, None)
        )
      )
      traj shouldBe trajectory
    }

    it("can be initialized with Array[Point], Array[Duration]") {
      val traj = Trajectory(pointArr, durationArr)
      traj shouldBe trajectory
    }

    it("can be initialized with Array[Point], Array[Duration] and Array[T]") {
      val traj = Trajectory(pointArr, durationArr, Array(None, None, None, None))
      traj shouldBe trajectory
    }

    it("can be initialized with Array[Point], Array[Duration], Array[T] and T") {
      val traj = Trajectory(pointArr, durationArr, Array(None, None, None, None), None)
      traj shouldBe trajectory
    }

    it("calculate speed with euclidean distance and the start of Duration (default)") {
      val traj = Trajectory(
        Array(
          (Point(0, 0), Duration(1626180345L, 1626180346L), None),
          (Point(0, 1), Duration(1626180346L, 1626180347L), None),
          (Point(0, 5), Duration(1626180347L, 1626180348L), None),
        )
      )

      def calSpeed(spatialDistances: Array[Double], temporalDistances: Array[Long]): Array[Double] =
        spatialDistances.zip(temporalDistances).map(x => x._1 / x._2)

      val speedArr = traj.mapConsecutive(calSpeed)
      speedArr shouldBe Array(1.0, 4.0)
    }


  }

}
