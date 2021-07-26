package instances


class TimeSeries[V, D](
  override val entries: Array[Entry[Polygon, V]],
  override val data: D)
  extends Instance[Polygon, V, D] {

  lazy val temporals: Array[Duration] = entries.map(_.temporal)

  require(validation,
    s"The temporal durations for TimeSeries must be disjoint with each other, " +
      s"bot got ${temporals.mkString("Array(", ", ", ")")}.")

  override def validation: Boolean = {
    temporals.sortBy(_.start).sliding(2).foreach {
      case Array(dur1, dur2) => if (dur1.end > dur2.start) return false
    }
    true
  }

  override def mapSpatial(f: Polygon => Polygon): TimeSeries[V, D] =
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

  override def mapData[D1](f: D => D1): TimeSeries[V, D1] =
    TimeSeries(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  // assumes disjoint temporal durations
  def getEntryIndex(timestampArr: Array[Long]): Array[Int] = {
    timestampArr.map(ts =>
      temporals.indexWhere(dur => dur.start == ts || dur.contains(ts))
    )
  }

  // assumes disjoint temporal durations
  def allocate(geomArr: Array[Point], timestampArr: Array[Long]): TimeSeries[Array[Point], D] = {
    require(geomArr.length == timestampArr.length, "the length of two arguments must match")

    val entryIndexArr = getEntryIndex(timestampArr)
    val entryIndexToGeom = geomArr.zip(entryIndexArr).groupBy(_._2).mapValues(x => x.map(_._1))

    val newValues = entries.zipWithIndex.map(entryWithIdx =>
      entryWithIdx._1.value match {
        case old: Array[Point] => old ++ entryIndexToGeom(entryWithIdx._2)
        case _ => throw new Exception("cannot allocate TimeSeries with Entry.value's type other than Array[Geometry]")
      }
    )

    val newSpatials = newValues.map{ geomArr =>
      val envelopes = geomArr.map(_.getEnvelopeInternal)
      val xMin = envelopes.map(_.getMinX).min
      val xMax = envelopes.map(_.getMaxX).max
      val yMin = envelopes.map(_.getMinY).min
      val yMax = envelopes.map(_.getMaxY).max
      Extent(xMin, yMin, xMax, yMax).toPolygon
    }
    val newEntries = (newSpatials, temporals, newValues).zipped.toArray.map(Entry(_))
    TimeSeries(newEntries, data)
  }

//  def merge(
//    other: TimeSeries[V, D],
//    dataCombiner: (D, D) => D
//  ): TimeSeries[V, D] = {
//    require(temporals sameElements other.temporals,
//      "cannot merge TimeSeries with different temporal structure")
//
////    val newValues = entries.map(_.value).map {
////      case old: Array[Geometry] => old ++ other.entries.map(_.value)
//////      case None => None
////      case _ => throw new Exception("cannot merge TimeSeries with Entry.value's type other than Array[Geometry]")
////    }
//
//    val newValues = entries.map(_.value) ++ other.entries.map(_.value)
//    val newSpatials = entries.map(_.spatial).zip(other.entries.map(_.spatial)).map(x =>
//      Extent(Array(x._1.getEnvelopeInternal, x._2.getEnvelopeInternal)).toPolygon
//    )
//    val newEntries = (newSpatials, temporals, newValues).zipped.toArray.map(Entry(_))
//    val newData = dataCombiner(data, other.data)
//    TimeSeries(newEntries, newData)
//  }

  def split(at: Long): (TimeSeries[V, D], TimeSeries[V, D]) = {
    require(temporals(0).end <= at && at <= temporals(dimension-1).start,
      s"the split timestamp should range from ${temporals(0).end} to ${temporals(temporals.length - 1).start}, " +
        s"but got $at")

    val splitIndex = temporals.indexWhere(_.start >= at)
    (TimeSeries(entries.slice(0, splitIndex), data),
      TimeSeries(entries.slice(splitIndex, dimension-1), data))
  }

  def select(targetDur: Array[Duration]): TimeSeries[V, D] =
    TimeSeries(entries.filter(x => targetDur.contains(x.temporal)), data)




}

object TimeSeries {
  def empty(durArr: Array[Duration]): TimeSeries[Array[Point], None.type] = {
    val durArrSorted = durArr.sortBy(_.start)
    TimeSeries(durArrSorted.map(x => Entry(Polygon.empty, x, Array.empty[Point])))
  }

  def apply[V, D](entries: Array[Entry[Polygon, V]], data: D): TimeSeries[V, D] = {
    val entriesSorted = entries.sortBy(_.temporal.start)
    new TimeSeries(entriesSorted, data)
  }

  def apply[V](entries: Array[Entry[Polygon, V]]): TimeSeries[V, None.type] = {
    val entriesSorted = entries.sortBy(_.temporal.start)
    new TimeSeries(entriesSorted, None)
  }

}