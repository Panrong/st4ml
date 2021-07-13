package instances

case class Event[S <: Geometry, V, D](
  entries: Array[Entry[S, V]],
  data: D)
  extends Instance[S, V, D] {

  require(entries.length == 1,
    s"The length of entries for Event should be 1, but got ${entries.length}")

  def mapSpatial[S1 <: Geometry](f: S => S1): Event[S1, V, D] =
    Event(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  def mapTemporal(f: Duration => Duration): Event[S, V, D] =
    Event(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  def mapValue[V1](f: V => V1): Event[S, V1, D] =
    Event(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  def mapEntries[S1 <: Geometry, V1](
                                      f1: S => S1,
                                      f2: Duration => Duration,
                                      f3: V => V1): Event[S1, V1, D] =
    Event(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  def mapData[D1](f: D => D1): Event[S, V, D1] =
    Event(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))
}

object Event {
  def apply[S <: Geometry, V](entries: Array[Entry[S, V]]): Event[S, V, None.type] =
    new Event(entries, None)

  def apply[S <: Geometry, V, D](s: S, t: Duration, v: V = None, d: D = None): Event[S, V, D] =
    new Event(Array(Entry(s, t, v)), d)
}