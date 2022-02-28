package instance

import st4ml.instances._
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

    it("can slide over entries, spatial or temporal") {
      val p1 = Point(0, 0)
      val p2 = Point(0, 1)
      val p3 = Point(0, 5)
      val d1 = Duration(1626180345L, 1626180346L)
      val d2 = Duration(1626180346L, 1626180347L)
      val d3 = Duration(1626180347L, 1626180348L)
      val e1 = Entry(p1, d1)
      val e2 = Entry(p2, d2)
      val e3 = Entry(p3, d3)
      val traj = Trajectory(Array(e1, e2, e3))

      traj.spatialSliding(2).toArray shouldBe Iterator(Array(p1, p2), Array(p2, p3)).toArray
      traj.temporalSliding(2).toArray shouldBe Iterator(Array(d1, d2), Array(d2, d3)).toArray
      traj.entrySliding(2).toArray shouldBe Iterator(Array(e1, e2), Array(e2, e3)).toArray
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

    it("can sort entries based on spatial") {
      val traj = Trajectory(
        Array(
          (Point(0, 2), Duration(1626180345L, 1626180346L), None),
          (Point(1, 1), Duration(1626180346L, 1626180347L), None),
          (Point(2, 0), Duration(1626180346L, 1626180347L), None))
      )

      val trajSortedByY = Trajectory(
        Array(
          (Point(2, 0), Duration(1626180346L, 1626180347L), None),
          (Point(1, 1), Duration(1626180346L, 1626180347L), None),
          (Point(0, 2), Duration(1626180345L, 1626180346L), None))
      )

      traj.sortBySpatial("x") shouldBe traj
      traj.sortBySpatial("y") shouldBe trajSortedByY
      traj.sortBySpatial("x", ascending = false) shouldBe trajSortedByY
      traj.sortBySpatial("y", ascending = false) shouldBe traj
    }

    it("can sort entries based on temporal") {
      val traj = Trajectory(
        Array(
          (Point(0, 2), Duration(1L, 10L), None),
          (Point(1, 1), Duration(2L, 7L), None),
          (Point(2, 0), Duration(3L, 4L), None))
      )

      val trajSortedByStart = traj
      val trajSortedByEnd = Trajectory(
        Array(
          (Point(2, 0), Duration(3L, 4L), None),
          (Point(1, 1), Duration(2L, 7L), None),
          (Point(0, 2), Duration(1L, 10L), None)
        )
      )
      val trajSortedByCenter = trajSortedByEnd
      val trajSortedByDuration = trajSortedByEnd

      traj.sortByTemporal("start") shouldBe trajSortedByStart
      traj.sortByTemporal("end") shouldBe trajSortedByEnd
      traj.sortByTemporal("center") shouldBe trajSortedByCenter
      traj.sortByTemporal("duration") shouldBe trajSortedByDuration
      traj.sortByTemporal("start", ascending = false) shouldBe trajSortedByStart.reverse
      traj.sortByTemporal("end", ascending = false) shouldBe trajSortedByEnd.reverse
      traj.sortByTemporal("center", ascending = false) shouldBe trajSortedByCenter.reverse
      traj.sortByTemporal("duration", ascending = false) shouldBe trajSortedByDuration.reverse
    }

    it("can sort entries with a UDF") {
      val traj = Trajectory(
        Array(
          (Point(0, 2), Duration(1L, 10L), None),
          (Point(1, 1), Duration(2L, 7L), None),
          (Point(2, 0), Duration(3L, 4L), None))
      )

      def entryDummySorter =
        (entry: Entry[Point, None.type]) =>
          entry.spatial.getX + entry.spatial.getY + entry.temporal.seconds

      val trajSorted = Trajectory(
        Array(
          (Point(2, 0), Duration(3L, 4L), None),
          (Point(1, 1), Duration(2L, 7L), None),
          (Point(0, 2), Duration(1L, 10L), None),
        )
      )

      traj.sortByEntry(entryDummySorter) shouldBe trajSorted
      traj.sortByEntry(entryDummySorter, ascending = false) shouldBe trajSorted.reverse
    }

    it("can merge with each other") {
      val entries1 = Array(
        (Point(0, 0), Duration(0L, 0L), None),
        (Point(1, 1), Duration(1L, 1L), None)
      )
      val entries2 = Array(
        (Point(2, 2), Duration(2L, 2L), None),
        (Point(3, 3), Duration(3L, 3L), None)
      )
      val entriesMerged = Array(
        (Point(0, 0), Duration(0L, 0L), None),
        (Point(1, 1), Duration(1L, 1L), None),
        (Point(2, 2), Duration(2L, 2L), None),
        (Point(3, 3), Duration(3L, 3L), None)
      )

      val traj1 = Trajectory(entries1)
      val traj2 = Trajectory(entries2)
      val trajMerged = Trajectory(entriesMerged)

      val trajDouble1 = Trajectory(entries1, 1.0)
      val trajDouble2 = Trajectory(entries2, 2.0)
      val trajDoubleMerged = Trajectory(entriesMerged, 3.0)

      val trajArray1 = Trajectory(entries1, Array(1, 2, 3))
      val trajArray2 = Trajectory(entries2, Array(4, 5, 6))
      val trajArrayMerged = Trajectory(entriesMerged, Array(1, 2, 3, 4, 5, 6))

      val trajMap1 = Trajectory(entries1, Map(1->"a", 2->"b", 3->"c"))
      val trajMap2 = Trajectory(entries2, Map(1->"a", 2->"b", 4->"d"))
      val trajMapMerged = Trajectory(entriesMerged, Map(1->"a", 2->"b", 3->"c", 4->"d"))

      traj1.merge(traj2, (_: None.type, _: None.type) => None) shouldBe trajMerged
      trajDouble1.merge(trajDouble2, (a: Double, b: Double) => a+b) shouldBe trajDoubleMerged

      // todo: how to check if two objects are the same
      trajArray1.merge(trajArray2, (a: Array[Int], b: Array[Int]) => a++b).entries shouldBe
        trajArrayMerged.entries
      trajArray1.merge(trajArray2, (a: Array[Int], b: Array[Int]) => a++b).data shouldBe
        trajArrayMerged.data
      trajMap1.merge(trajMap2, (a: Map[Int, String], b: Map[Int, String]) => a++b).entries shouldBe
        trajMapMerged.entries
      trajMap1.merge(trajMap2, (a: Map[Int, String], b: Map[Int, String]) => a++b).data shouldBe
        trajMapMerged.data
    }

    it("can merge with each other and sort") {
      val entries1 = Array(
        (Point(1, 1), Duration(1L, 1L), None),
        (Point(0, 0), Duration(0L, 0L), None)
      )
      val entries2 = Array(
        (Point(3, 3), Duration(3L, 3L), None),
        (Point(2, 2), Duration(2L, 2L), None)
      )
      val entriesMerged = Array(
        (Point(0, 0), Duration(0L, 0L), None),
        (Point(1, 1), Duration(1L, 1L), None),
        (Point(2, 2), Duration(2L, 2L), None),
        (Point(3, 3), Duration(3L, 3L), None)
      )

      val traj1 = Trajectory(entries1)
      val traj2 = Trajectory(entries2)
      val trajMerged = Trajectory(entriesMerged)

      traj1.mergeAndSort(
        traj2,
        (_: None.type, _: None.type) => None,
        (entry: Entry[Point, None.type]) => entry.temporal.start
      ) shouldBe trajMerged
    }


  }

}
