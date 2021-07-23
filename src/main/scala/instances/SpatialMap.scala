package instances

class SpatialMap[V, D](
  override val entries: Array[Entry[Polygon, V]],
  override val data: D)
  extends Instance[Polygon, V, D] {

 lazy val spatials: Array[Polygon] = entries.map(_.spatial)

  override def validation: Boolean = ???

  override def mapSpatial(f: Polygon => Polygon): SpatialMap[V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)


  override def mapTemporal(f: Duration => Duration): SpatialMap[V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): SpatialMap[V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  override def mapEntries[V1](
    f1: Polygon => Polygon,
    f2: Duration => Duration,
    f3: V => V1): SpatialMap[V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapData[D1](f: D => D1): SpatialMap[V, D1] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))


}

object SpatialMap {
//  def empty(extentArr: Array[Extent]): SpatialMap[None.type, None.type] = {
//    val extentArrSorted = extentArr.sorted
//    SpatialMap(extentArrSorted.map(x => Entry(x.toPolygon, Duration.empty)))
//  }

  def apply[V, D](entries: Array[Entry[Polygon, V]], data: D): SpatialMap[V, D] =
    new SpatialMap(entries, data)

  def apply[V](entries: Array[Entry[Polygon, V]]): SpatialMap[V, None.type] =
    new SpatialMap(entries, None)
}