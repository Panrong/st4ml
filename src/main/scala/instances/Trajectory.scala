package instances

case class Trajectory[V, D](
  entries: Array[Entry[Point, V]],
  data: D)
  extends Instance[Point, V, D] {

  def mapEntries[V1](
    f1: Point => Point,
    f2: Duration => Duration,
    f3: V => V1): Trajectory[V1, D] =
    Trajectory(
      entries.map(entry => Entry(
        f1(entry.spatial),
        f2(entry.temporal),
        f3(entry.value))
      ),
      data
    )

  def mapData[D1](f: D => D1): Trajectory[V, D1] =
    Trajectory(entries, f(data))



}
