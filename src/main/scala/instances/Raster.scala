package instances

case class Raster[S <: Geometry, V, D](
  entries: Array[Entry[S, V]],
  data: D)
  extends Instance[S, V, D] {

  def mapSpatial(f: S => S): Raster[S, V, D] =
    Raster(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  def mapTemporal(f: Duration => Duration): Raster[S, V, D] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  def mapValue[V1](f: V => V1): Raster[S, V1, D] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  def mapEntries[V1](
    f1: S => S,
    f2: Duration => Duration,
    f3: V => V1): Raster[S, V1, D] =
    Raster(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  def mapData[D1](f: D => D1): Raster[S, V, D1] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

}