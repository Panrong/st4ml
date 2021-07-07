package instances

case class SpatialMap[V, D](
  entries: Array[Entry[Polygon, V]],
  data: D)
  extends Instance[Polygon, V, D] {

  def mapEntries[V1](
    f1: Polygon => Polygon,
    f2: Duration => Duration,
    f3: V => V1): SpatialMap[V1, D] =
    SpatialMap(
      entries.map(entry => Entry(
        f1(entry.spatial),
        f2(entry.temporal),
        f3(entry.value))
      ),
      data
    )

  def mapData[D1](f: D => D1): SpatialMap[V, D1] =
    SpatialMap(entries, f(data))

}