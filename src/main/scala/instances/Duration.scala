package instances

case class DurationRangeError(msg: String) extends Exception(msg)

case class Duration(start: Long, end: Long) {
  // Validation
  if (start > end) {
    throw ExtentRangeError(s"Invalid Duration: start must be smaller or equal to end (start=$start, end=$end)")
  }

  def isInstant: Boolean = start == end

  def ==(other: Duration): Boolean =
    start == other.start && end == other.end

  def seconds: Long = end - start

  def hours: Double = seconds / 60

  def days: Double = seconds / (60 * 24)

  def center: Long = (start + end) / 2

  def intersects(timestampInSecond: Long): Boolean =
    timestampInSecond == start ||
      timestampInSecond == end ||
      (timestampInSecond > start && timestampInSecond < end)

  def intersects(dur: Duration): Boolean =
    !(dur.end < start || end < dur.start)

  def contains(timestampInSecond: Long): Boolean =
    start < timestampInSecond && timestampInSecond < end

  def contains(dur: Duration): Boolean =
    start < dur.start && dur.end < end

  def intersection(other: Duration): Duration = {
    val newStart = if (start > other.start) start else other.start
    val newEnd = if (end < other.end) end else other.end
    Duration(newStart, newEnd)
  }

  def plusSeconds(deltaStart: Long, deltaEnd: Long): Duration = Duration(
    start+deltaStart,
    end+deltaEnd
  )


  override def toString: String =
    s"Duration($start, $end)"

}

object Duration {
  def apply(epochSecond1: Instant, epochSecond2: Instant): Duration =
    new Duration(epochSecond1.getEpochSecond, epochSecond2.getEpochSecond)

  def apply(epochSecond1: String, epochSecond2: String): Duration =
    new Duration(Instant(epochSecond1).getEpochSecond, Instant(epochSecond2).getEpochSecond)

  def apply(epochSecond: Long): Duration = {
    new Duration(epochSecond, epochSecond)
  }

  def apply(epochSecond: Instant): Duration = {
    val epochSecondLong = epochSecond.getEpochSecond
    new Duration(epochSecondLong, epochSecondLong)
  }

  def apply(epochSecond: String): Duration = {
    val epochSecondLong = Instant(epochSecond).getEpochSecond
    new Duration(epochSecondLong, epochSecondLong)
  }

  def apply(durations: Array[Duration]): Duration = {
    val start = durations.map(_.start).min
    val end = durations.map(_.end).max
    new Duration(start, end)
  }
}



