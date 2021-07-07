package instances

case class Raster[S, V, D](
  entries: Array[Entry[S, V]],
  data: D)
  extends Instance[S, V, D] {

  def mapEntries[S1, V1](
    f1: S => S1,
    f2: Duration => Duration,
    f3: V => V1): Raster[S1, V1, D] =
    Raster(
      entries.map(entry => Entry(
        f1(entry.spatial),
        f2(entry.temporal),
        f3(entry.value))
      ),
      data
    )

  def mapData[D1](f: D => D1): Raster[S, V, D1] =
    Raster(entries, f(data))

}