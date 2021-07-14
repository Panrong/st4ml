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

    it("calculates speed with euclidean distance and the start of Duration (default)") {
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

    it("calculates spatial distance with great circle distance") {
      val point1 = Point(120.0891742646, 30.3444758047)
      val point2 = Point(119.95031023, 30.271382)
      val point3 = Point(119.9452875121, 30.2812551285)

      val traj = Trajectory(
        Array(
          (point1, Duration(1626180345L, 1626180346L), None),
          (point2, Duration(1626180346L, 1626180347L), None),
          (point3, Duration(1626180347L, 1626180348L), None),
        )
      )

      val spatialDistance = traj.consecutiveSpatialDistance("greatCircle")

      spatialDistance shouldBe Array(Distances.greatCircleDistance(point1.getX, point1.getY, point2.getX, point2.getY),
        Distances.greatCircleDistance(point2.getX, point2.getY, point3.getX, point3.getY))
    }

    it("calculates spatial distance with an user-defined function") {
      val point1 = Point(120.0891742646, 30.3444758047)
      val point2 = Point(119.95031023, 30.271382)
      val point3 = Point(119.9452875121, 30.2812551285)

      val traj = Trajectory(
        Array(
          (point1, Duration(1626180345L, 1626180346L), None),
          (point2, Duration(1626180346L, 1626180347L), None),
          (point3, Duration(1626180347L, 1626180348L), None),
        )
      )

      def func1: (Point, Point) => Double =
        (p1: Point, p2: Point) => math.abs(p1.getX - p2.getX) + math.abs(p1.getY - p2.getY)

      val spatialDistance = traj.consecutiveSpatialDistance(func1)

      spatialDistance shouldBe Array(func1(point1, point2), func1(point2, point3))
    }

    it("calculates temporal distance with end of duration") {
      val traj = Trajectory(
        Array(
          (Point(0, 0), Duration(1626180345L, 1626180346L), None),
          (Point(0, 1), Duration(1626180346L, 1626180347L), None),
          (Point(0, 5), Duration(1626180347L, 1626180348L), None),
        )
      )

      val temporalDistances = traj.consecutiveTemporalDistance("end")
      temporalDistances shouldBe Array(1.0, 1.0)
    }

    it("calculates temporal distance with an user-defined function") {
      val traj = Trajectory(
        Array(
          (Point(0, 0), Duration(1626180345L, 1626180346L), None),
          (Point(0, 1), Duration(1626180346L, 1626180347L), None),
          (Point(0, 5), Duration(1626180347L, 1626180348L), None),
        )
      )

      val func2: (Duration, Duration) => Long =
        (d1: Duration, d2: Duration) => math.abs(d1.start - d2.start) + math.abs(d1.end - d2.end)

      val temporalDistances = traj.consecutiveTemporalDistance(func2)
      temporalDistances shouldBe Array(2L, 2L)
    }


  }

}
