package instances

import GeometryImplicits.withExtraPointOps

class Trajectory[V, D](
  override val entries: Array[Entry[Point, V]],
  override val data: D)
  extends Instance[Point, V, D] {

  require(validation,
    s"The length of entries for Trajectory at least 2, but got ${entries.length}")

  override def validation: Boolean =
    entries.length > 1

  override def mapSpatial(f: Point => Point): Trajectory[V, D] =
    Trajectory(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  override def mapTemporal(f: Duration => Duration): Trajectory[V, D] =
    Trajectory(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): Trajectory[V1, D] =
    Trajectory(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  override def mapEntries[V1](
    f1: Point => Point,
    f2: Duration => Duration,
    f3: V => V1): Trajectory[V1, D] =
    Trajectory(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapData[D1](f: D => D1): Trajectory[V, D1] =
    Trajectory(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  def entrySliding(n: Int): Iterator[Array[Entry[Point, V]]] =
    entries.sliding(n)

  def spatialSliding(n: Int): Iterator[Array[Point]] =
    entries.map(_.spatial).sliding(n)

  def temporalSliding(n: Int): Iterator[Array[Duration]] =
    entries.map(_.temporal).sliding(n)

  def consecutiveSpatialDistance(metric: String): Array[Double] = {
    metric match {
      case "euclidean" => entries.map(_.spatial).sliding(2).map {
        case Array(p1, p2) => p1.euclidean(p2)
      }.toArray
      case "greatCircle" => entries.map(_.spatial).sliding(2).map {
        case Array(p1, p2) => p1.greatCircle(p2)
      }.toArray
      case _ => throw new Exception(
        s"""Invalid metric: the input metric should be either "euclidean" or "greatCircle"
          | but got $metric
          |""".stripMargin)
    }
  }

  def consecutiveSpatialDistance(metric: (Point, Point) => Double): Array[Double] = {
    spatialSliding(2).map {
        case Array(p1, p2) => metric(p1, p2)
      }.toArray
  }

  def consecutiveTemporalDistance(metric: String): Array[Long] = {
    metric match {
      case "start" => entries.map(_.temporal.start).sliding(2).map {
        case Array(t1, t2) => t2 - t1
      }.toArray
      case "end" => entries.map(_.temporal.end).sliding(2).map {
        case Array(t1, t2) => t2 - t1
      }.toArray
      case _ => throw new Exception(
        s"""Invalid metric: the input metric should be either "start" or "end"
           | but got $metric
           |""".stripMargin)
    }
  }

  def consecutiveTemporalDistance(metric: (Duration, Duration) => Long): Array[Long] = {
    temporalSliding(2).map {
      case Array(dur1, dur2) => metric(dur1, dur2)
    }.toArray
  }

  def mapConsecutive(
    f: (Array[Double], Array[Long]) => Array[Double],
    spatialMetric: String = "euclidean",
    temporalMetric: String = "start"
  ): Array[Double] = {
    val csd = consecutiveSpatialDistance(spatialMetric)
    val ctd = consecutiveTemporalDistance(temporalMetric)
    f(csd, ctd)
  }

  def mapConsecutive(
    f: (Array[Double], Array[Long]) => Array[Double],
    spatialMetric: (Point, Point) => Double,
    temporalMetric: (Duration, Duration) => Long
  ): Array[Double] = {
    val csd = consecutiveSpatialDistance(spatialMetric)
    val ctd = consecutiveTemporalDistance(temporalMetric)
    f(csd, ctd)
  }

  def sortBySpatial(metric: String, ascending: Boolean = true): Trajectory[V, D] = {
    val sign = if (ascending) 1 else -1
    metric match {
      case "x" => Trajectory(entries.sortBy(_.spatial.x * sign), data)
      case "y" => Trajectory(entries.sortBy(_.spatial.y * sign), data)
      case _ => throw new Exception(
        s"""Invalid metric: the input metric should be either "x" or "y"
           | but got $metric
           |""".stripMargin)
    }
  }

  def sortByTemporal(metric: String, ascending: Boolean = true): Trajectory[V, D] = {
    val sign = if (ascending) 1 else -1
    metric match {
      case "start" => Trajectory(entries.sortBy(_.temporal.start * sign), data)
      case "end" => Trajectory(entries.sortBy(_.temporal.end * sign), data)
      case "center" => Trajectory(entries.sortBy(_.temporal.center * sign), data)
      case "duration" => Trajectory(entries.sortBy(_.temporal.seconds * sign), data)
      case _ => throw new Exception(
        s"""Invalid metric: the input metric should be "start", "end", "center" or "duration"
           | but got $metric
           |""".stripMargin)
    }
  }

  def sortByEntry(
    entrySorter: Entry[Point, V] => Double,
    ascending: Boolean = true): Trajectory[V, D] = {
    val sign = if (ascending) 1 else -1
    Trajectory(entries.sortBy(x => entrySorter(x)  * sign), data)
  }

  def reverse: Trajectory[V, D] =
    Trajectory(entries.reverse, data)

  def merge(
    other: Trajectory[V, D],
    dataCombiner: (D, D) => D
  ): Trajectory[V, D] =
    Trajectory(entries ++ other.entries, dataCombiner(data, other.data))

  def mergeAndSort(
    other: Trajectory[V, D],
    dataCombiner: (D, D) => D,
    entrySorter: Entry[Point, V] => Double
  ): Trajectory[V, D] =
    Trajectory((entries ++ other.entries).sortBy(x => entrySorter(x)),
      dataCombiner(data, other.data))

}

object Trajectory {
  def apply[V, D](entries: Array[Entry[Point, V]], data: D): Trajectory[V, D] =
    new Trajectory(entries, data)

  def apply[V](entries: Array[Entry[Point, V]]): Trajectory[V, None.type] =
    new Trajectory(entries, None)

  def apply[V, D](arr: Array[(Point, Duration, V)], d: D): Trajectory[V, D] =
    new Trajectory(arr.map(Entry(_)), d)

  def apply[V](arr: Array[(Point, Duration, V)]): Trajectory[V, None.type] =
    new Trajectory(arr.map(Entry(_)), None)

  def apply[V, D](
    pointArr: Array[Point],
    durationArr: Array[Duration],
    valueArr: Array[V],
    d2: D): Trajectory[V, D] = {
    require(pointArr.length == durationArr.length,
      "the length of second argument should match the length of first argument")
    require(pointArr.length == valueArr.length,
      "the length of third argument should match the length of first argument")
    val entries = (pointArr, durationArr, valueArr).zipped.toArray.map(Entry(_))
    new Trajectory(entries, d2)
  }

  def apply[V](
    pointArr: Array[Point],
    durationArr: Array[Duration],
    valueArr: Array[V]): Trajectory[V, None.type] = {
    require(pointArr.length == durationArr.length,
      "the length of second argument should match the length of first argument")
    require(pointArr.length == valueArr.length,
      "the length of third argument should match the length of first argument")
    val entries = (pointArr, durationArr, valueArr).zipped.toArray.map(Entry(_))
    new Trajectory(entries, None)
  }

  // todo: delete or find a way around
//  def apply[D](
//    pointArr: Array[Point],
//    durationArr: Array[Duration],
//    d: D): Trajectory[None.type, D] = {
//    require(pointArr.length == durationArr.length,
//      "the length of second argument should match the length of first argument")
//    val entries = (pointArr, durationArr).zipped.toArray.map(Entry(_))
//    new Trajectory(entries, d)
//  }

  def apply(
    pointArr: Array[Point],
    durationArr: Array[Duration]): Trajectory[None.type, None.type] = {
    require(pointArr.length == durationArr.length,
      "the length of second argument should match the length of first argument")
    val entries = (pointArr, durationArr).zipped.toArray.map(Entry(_))
    new Trajectory(entries, None)
  }

}
