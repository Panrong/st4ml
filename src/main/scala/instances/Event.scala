package instances

case class Event[S <: Geometry, V, D](
   val entries: Array[Entry[S, V]],
   val data: D)
  extends Instance[S, V, D] {

  require(validation,
    s"The length of entries for Event should be 1, but got ${entries.length}")

  override def validation: Boolean =
    entries.length == 1

  override def mapSpatial(f: S => S): Event[S, V, D] =
    Event(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  override def mapTemporal(f: Duration => Duration): Event[S, V, D] =
    Event(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): Event[S, V1, D] =
    Event(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  override def mapEntries[V1](
    f1: S => S,
    f2: Duration => Duration,
    f3: V => V1): Event[S, V1, D] =
    Event(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapData[D1](f: D => D1): Event[S, V, D1] =
    Event(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  override def mapEntries[V1](f: Entry[S, V] => Entry[S, V1]): Event[S, V1, D] =
    Event(entries.map(f(_)), data)

  override def toGeometry: Geometry =
    entries(0).spatial
}

object Event {
  def apply[S <: Geometry, V, D](entries: Array[Entry[S, V]], data: D): Event[S, V, D] =
    new Event(entries, data)

  def apply[S <: Geometry, V](entries: Array[Entry[S, V]]): Event[S, V, None.type] =
    new Event(entries, None)

  def apply[S <: Geometry, V, D](s: S, t: Duration, v: V = None, d: D = None): Event[S, V, D] =
    new Event(Array(Entry(s, t, v)), d)
}