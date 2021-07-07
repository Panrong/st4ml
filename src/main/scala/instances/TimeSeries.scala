package instances

//todo: why "extent" is ok?
case class TimeSeries[V, D](
  entries: Array[Entry[Extent, V]],
  data: D)
  extends Instance[Extent, V, D] {

  def mapEntries[V1](
    f1: Extent => Extent,
    f2: Duration => Duration,
    f3: V => V1): TimeSeries[V1, D] =
    TimeSeries(
      entries.map(entry => Entry(
        f1(entry.spatial),
        f2(entry.temporal),
        f3(entry.value))
      ),
      data
    )

  def mapData[D1](f: D => D1): TimeSeries[V, D1] =
    TimeSeries(entries, f(data))

}