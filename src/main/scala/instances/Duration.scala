package instances

case class DurationRangeError(msg: String) extends Exception(msg)

case class Duration(start: Instant, end: Instant) {
  // Validation
  if (start.isAfter(end)) {
    throw ExtentRangeError(s"Invalid Duration: start must be smaller or equal to end (start=$start, end=$end)")
  }

  def isEmpty: Boolean = start == end

  def ==(other: Duration): Boolean =
    start == other.start && end == other.end

  def seconds: Long = end.getEpochSecond - start.getEpochSecond

  def hours: Double = seconds / 60

  def days: Double = seconds / (60 * 24)

  def intersects(t: Instant): Boolean =
    t.equals(start) ||
      t.equals(end) ||
      (t.isAfter(start) && t.isBefore(end))

  def contains(t: Instant): Boolean =
    t.isAfter(start) && t.isBefore(end)

  def intersection(other: Duration): Duration = {
    val newStart = if (start.isAfter(other.start)) start else other.start
    val newEnd = if (end.isBefore(other.end)) end else other.end
    Duration(newStart, newEnd)
  }

  def plusSeconds(deltaStart: Long, deltaEnd: Long): Duration = Duration(
    start.plusSeconds(deltaStart),
    end.plusSeconds(deltaEnd)
  )

}

object Duration {
  def apply(epochSecond1: Long, epochSecond2: Long): Duration =
    new Duration(Instant(epochSecond1), Instant(epochSecond2))

  def apply(epochSecond1: String, epochSecond2: String): Duration =
    new Duration(Instant(epochSecond1), Instant(epochSecond2))

  def apply(durations: Array[Duration]): Duration = {
    val start = durations.map(_.start).min
    val end = durations.map(_.end).max
    new Duration(start, end)
  }
}



