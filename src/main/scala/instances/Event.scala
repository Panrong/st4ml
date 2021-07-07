package instances

case class InvalidEventError(msg: String) extends Exception(msg)

case class Event[S, V, D](
  entries: Array[Entry[S, V]],
  data: D)
  extends Instance[S, V, D] {

  if (entries.length != 1) {
    throw InvalidEventError(s"The length of entries for Event should be 1, but got ${entries.length}")
  }

  def mapEntries[S1, V1](
    f1: S => S1,
    f2: Duration => Duration,
    f3: V => V1): Event[S1, V1, D] =
    Event(
      entries.map(entry => Entry(
        f1(entry.spatial),
        f2(entry.temporal),
        f3(entry.value))
      ),
      data
    )

  def mapData[D1](f: D => D1): Event[S, V, D1] =
    Event(entries, f(data))

}
