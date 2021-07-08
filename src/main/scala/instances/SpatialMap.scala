package instances

case class SpatialMap[V, D](
  entries: Array[Entry[Polygon, V]],
  data: D)
  extends Instance[Polygon, V, D] {

  def mapSpatial(f: Polygon => Polygon): SpatialMap[V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)


  def mapTemporal(f: Duration => Duration): SpatialMap[V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  def mapValue[V1](f: V => V1): SpatialMap[V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  def mapEntries[V1](
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

  def mapData[D1](f: D => D1): SpatialMap[V, D1] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))
}