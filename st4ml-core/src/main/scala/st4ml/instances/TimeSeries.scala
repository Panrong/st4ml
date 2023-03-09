package st4ml.instances

import scala.reflect.ClassTag

class TimeSeries[V, D](
                        override val entries: Array[Entry[Polygon, V]],
                        override val data: D)
  extends Instance[Polygon, V, D] {

  lazy val temporals: Array[Duration] = entries.map(_.temporal)
  lazy val spatials: Array[Polygon] = temporals.map(_ => Polygon.empty)
  lazy val temporal: Duration = Duration(temporals.head.start, temporals.last.end)
  var rTree: Option[RTree[Polygon, String]] = None
  require(validation,
    s"The length of entries for TimeSeries should be at least 1, but got ${entries.length}")

  def isTemporalDisjoint: Boolean = {
    if (temporals.length > 1) {
      temporals.sortBy(_.start).sliding(2).foreach {
        case Array(dur1, dur2) => if (dur1.end > dur2.start) return false
      }
    }
    true
  }

  def isRegular: Boolean = temporals.forall(x => x.seconds == temporals.head.seconds) &&
    isTemporalDisjoint && this.duration.seconds == temporals.map(_.seconds).sum

  def mapSpatial(f: Polygon => Polygon): TimeSeries[V, D] =
    TimeSeries(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  override def mapTemporal(f: Duration => Duration): TimeSeries[V, D] =
    TimeSeries(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): TimeSeries[V1, D] =
    TimeSeries(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  def mapValuePlus[V1](f: (V, Polygon, Duration) => V1): TimeSeries[V1, D] = {
    val newEntries = entries.map(entry => {
      val spatial = entry.spatial
      val temporal = entry.temporal
      Entry(spatial, temporal, f(entry.value, spatial, temporal))
    })
    TimeSeries(newEntries, data)
  }

  override def mapEntries[V1](
                               f1: Polygon => Polygon,
                               f2: Duration => Duration,
                               f3: V => V1): TimeSeries[V1, D] =
    TimeSeries(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapEntries[V1](f: Entry[Polygon, V] => Entry[Polygon, V1]): TimeSeries[V1, D] =
    TimeSeries(entries.map(f(_)), data)

  override def mapData[D1](f: D => D1): TimeSeries[V, D1] =
    TimeSeries(entries, f(data))

  def mapDataPlus[D1](f: (D, Polygon, Duration) => D1): TimeSeries[V, D1] =
    TimeSeries(entries, f(data, extent.toPolygon, temporal))

  // methods for calculating the corresponding index/indices of bins for given query
  def getBinIndex(bins: Array[Duration], queryArr: Array[Long]): Array[Array[Int]] =
    queryArr.map(q =>
      Array(bins.indexWhere(_.intersects(q)))
    )

  def getBinIndices(bins: Array[Duration], queryArr: Array[Duration]): Array[Array[Int]] = {
    queryArr.map(q =>
      bins.zipWithIndex.filter(_._1.intersects(q)).map(_._2)
    )
  }

  /**
   * Find the indices of temporal bins for each element of timeArr.
   *
   * Noted that:
   * 1. An element is considered in a temporal bin as long as they intersect (including duration boundaries)
   * 2. When temporal bins are disjoint and the input timeArr is Array[Long], each element in the timeArr would only
   * belong to one temporal bin. Therefore, Utils.getBinIndex is used instead of Utils.getBinIndices
   *
   * @param timeArr each element represents a timestamp or a duration
   * @return the indices of temporal bins
   */
  def getTemporalIndex(timeArr: Array[_]): Array[Array[Int]] = {
    timeArr match {
      case durArr: Array[Duration] => getBinIndices(temporals, durArr)
      case timestampArr: Array[Long] =>
        if (isTemporalDisjoint)
          getBinIndex(temporals, timestampArr)
        else {
          getBinIndices(temporals, timestampArr.map(Duration(_)))
        }
      case _ => throw new IllegalArgumentException(
        "Unsupported type of input argument in method getTemporalIndex; " +
          "inputArr should be either Array[Long] or Array[Duration]")
    }
  }

  def getTemporalIndexRTree(timeArr: Array[Duration]): Array[Array[Int]] =
    timeArr.map(query => rTree.get.range1d((query.start, query.end)).map(_._2.toInt))

  def getTemporalIndexToObj[T: ClassTag](objArr: Array[T], timeArr: Array[_]): Map[Int, Array[T]] = {
    if (timeArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getTemporalIndex(timeArr)
      objArr.zip(indices)
        .filter(_._2.length > 0)
        .flatMap { case (geom, indexArr) =>
          for {
            idx <- indexArr
          } yield (geom, idx)
        }
        .groupBy(_._2)
        .mapValues(x => x.map(_._1))
    }
  }

  def getTemporalIndexToObjRTree[T: ClassTag](objArr: Array[T], timeArr: Array[Duration]): Map[Int, Array[T]] = {
    if (timeArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getTemporalIndexRTree(timeArr)
      objArr.zip(indices)
        .filter(_._2.length > 0)
        .flatMap { case (geom, indexArr) =>
          for {
            idx <- indexArr
          } yield (geom, idx)
        }
        .groupBy(_._2)
        .mapValues(x => x.map(_._1))
    }
  }

  def createTimeSeries[T: ClassTag](
                                     temporalIndexToObj: Map[Int, Array[T]],
                                     computePolygonFunc: Array[T] => Polygon
                                   ): TimeSeries[Array[T], D] = {
    if (temporalIndexToObj.nonEmpty) {
      val newValues = entries.zipWithIndex.map(entryWithIdx =>
        if (temporalIndexToObj.contains(entryWithIdx._2)) {
          entryWithIdx._1.value.asInstanceOf[Array[T]] ++ temporalIndexToObj(entryWithIdx._2)
        }
        else {
          entryWithIdx._1.value.asInstanceOf[Array[T]]
        }
      )
      //     val newSpatials = newValues.map { newGeomArr => computePolygonFunc(newGeomArr) }
      val newEntries = (spatials, temporals, newValues).zipped.toArray.map(Entry(_))
      TimeSeries(newEntries, data)
    }
    else {
      this.asInstanceOf[TimeSeries[Array[T], D]]
    }
  }

  def attachGeometry[T <: Geometry : ClassTag](geomArr: Array[T], timestampArr: Array[Long])
                                              (implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    require(geomArr.length == timestampArr.length,
      "the length of two arguments must match")

    val entryIndexToGeom = getTemporalIndexToObj(geomArr, timestampArr)
    createTimeSeries(entryIndexToGeom, Utils.getPolygonFromGeometryArray)
  }

  def attachGeometry[T <: Geometry : ClassTag](geomArr: Array[T], durationArr: Array[Duration])
                                              (implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    require(geomArr.length == durationArr.length,
      "the length of two arguments must match")

    val entryIndexToGeom = getTemporalIndexToObj(geomArr, durationArr)
    createTimeSeries(entryIndexToGeom, Utils.getPolygonFromGeometryArray)
  }

  def attachInstance[T <: Instance[_, _, _] : ClassTag](instanceArr: Array[T], timestampArr: Array[Long])
                                                       (implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    require(instanceArr.length == timestampArr.length,
      "the length of two arguments must match")

    val entryIndexToInstance = getTemporalIndexToObj(instanceArr, timestampArr)
    createTimeSeries(entryIndexToInstance, Utils.getPolygonFromInstanceArray)
  }

  def attachInstance[T <: Instance[_, _, _] : ClassTag](instanceArr: Array[T], durationArr: Array[Duration])
                                                       (implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    require(instanceArr.length == durationArr.length,
      "the length of two arguments must match")

    val entryIndexToInstance = getTemporalIndexToObj(instanceArr, durationArr)
    createTimeSeries(entryIndexToInstance, Utils.getPolygonFromInstanceArray)
  }

  def attachInstance[T <: Instance[_, _, _] : ClassTag](instanceArr: Array[T])
                                                       (implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    val durationArr = instanceArr.map(_.duration)
    attachInstance(instanceArr, durationArr)
  }

  def attachInstanceRTree[T <: Instance[_, _, _] : ClassTag](instanceArr: Array[T])
                                                            (implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    // in called in a converter, a broadcast Rtree will pass in and this line will not execute
    if (rTree.isEmpty) rTree = Some(Utils.buildRTree(temporals))
    val durationArr = instanceArr.map(_.duration)
    val entryIndexToInstance = getTemporalIndexToObjRTree(instanceArr, durationArr)
    createTimeSeries(entryIndexToInstance, Utils.getPolygonFromInstanceArray)
  }

  // todo: handle different order of the same temporals
  def merge[T: ClassTag](
                          other: TimeSeries[Array[T], _]
                        )(implicit ev: Array[T] =:= V): TimeSeries[Array[T], None.type] = {
    require(temporals sameElements other.temporals,
      "cannot merge TimeSeries with different temporal structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newSpatials = entries.map(_.spatial).zip(other.entries.map(_.spatial)).map(x =>
      Utils.getPolygonFromGeometryArray(Array(x._1, x._2))
    )
    val newEntries = (newSpatials, temporals, newValues).zipped.toArray.map(Entry(_))
    TimeSeries(newEntries, None)
  }

  def merge[T](
                other: TimeSeries[T, D],
                valueCombiner: (V, T) => V,
                dataCombiner: (D, D) => D
              ): TimeSeries[V, D] = {
    require(temporals sameElements other.temporals,
      "cannot merge TimeSeries with different temporal structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      valueCombiner(x._1, x._2)
    )
    val newSpatials = entries.map(_.spatial).zip(other.entries.map(_.spatial)).map(x =>
      Utils.getPolygonFromGeometryArray(Array(x._1, x._2))
    )
    val newEntries = (newSpatials, temporals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    TimeSeries(newEntries, newData)
  }

  def merge[T: ClassTag](
                          other: TimeSeries[Array[T], D],
                          dataCombiner: (D, D) => D
                        )(implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = {
    require(temporals sameElements other.temporals,
      "cannot merge TimeSeries with different temporal structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newSpatials = entries.map(_.spatial).zip(other.entries.map(_.spatial)).map(x =>
      Utils.getPolygonFromGeometryArray(Array(x._1, x._2))
    )
    val newEntries = (newSpatials, temporals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    TimeSeries(newEntries, newData)
  }

  def split(at: Long): (TimeSeries[V, D], TimeSeries[V, D]) = {
    require(temporals(0).end <= at && at <= temporals(entryLength - 1).start,
      s"the split timestamp should range from " +
        s"${temporals(0).end} to ${temporals(temporals.length - 1).start}, but got $at")

    val splitIndex = temporals.indexWhere(_.start >= at)
    val (entriesPart1, entriesPart2) = entries.splitAt(splitIndex)
    (TimeSeries(entriesPart1, data),
      TimeSeries(entriesPart2, data))
  }

  def select(targetDur: Array[Duration]): TimeSeries[V, D] =
    TimeSeries(entries.filter(x => targetDur.contains(x.temporal)), data)

  def append(other: TimeSeries[V, _]): TimeSeries[V, None.type] =
    TimeSeries(entries ++ other.entries, None)

  def append(other: TimeSeries[V, D], dataCombiner: (D, D) => D): TimeSeries[V, D] =
    TimeSeries(entries ++ other.entries, dataCombiner(data, other.data))

  override def toGeometry: Polygon = extent.toPolygon

  // sort by tMin  of the temporal of each entry
  def sorted: TimeSeries[V, D] = {
    val newEntries = entries.sortBy(x => x.temporal.start)
    new TimeSeries[V, D](newEntries, data)
  }

  override def setData[D1](newData: D1): TimeSeries[V, D1] = new TimeSeries(entries, newData)

}

object TimeSeries {
  //todo: Polygon.empty to extent error
  def empty[T: ClassTag](durArr: Array[Duration]): TimeSeries[Array[T], None.type] = {
    TimeSeries(durArr.map(x => Entry(Polygon.empty, x, Array.empty[T])))
  }

  def apply[V, D](entries: Array[Entry[Polygon, V]], data: D): TimeSeries[V, D] = {
    new TimeSeries(entries, data)
  }

  def apply[V](entries: Array[Entry[Polygon, V]]): TimeSeries[V, None.type] = {
    new TimeSeries(entries, None)
  }

  def apply[V, D](durArr: Array[Duration], valArr: Array[V], data: D = None): TimeSeries[V, D] = {
    assert(durArr.length == valArr.length,
      s"The length of durArr and valArr have to be identical. Got ${durArr.length} and ${valArr.length}")
    val entries = durArr.zip(valArr).map(x => Entry(Polygon.empty, x._1, x._2))
    new TimeSeries[V, D](entries, data)
  }

}