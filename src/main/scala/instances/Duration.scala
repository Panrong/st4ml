package instances

case class DurationRangeError(msg: String) extends Exception(msg)

case class Duration(start: Instant, end: Instant) {

  // Validation
  if (start.isAfter(end)) {
    throw ExtentRangeError(s"Invalid Duration: start must be smaller or equal to end (start=$start, end=$end)")
  }

  def isEmpty: Boolean = start == end

  def intersects(t: Instant): Boolean =
    t.equals(start) ||
      t.equals(end) ||
      (t.isAfter(start) && t.isBefore(end))

  def contains(t: Instant): Boolean =
    (t.isAfter(start) && t.isBefore(end))

  def intersection(other: Duration): Duration = {
    val newStart = if (start.isAfter(other.start)) start else other.start
    val newEnd = if (end.isBefore(other.end)) end else other.end
    Duration(newStart, newEnd)
  }

  def expandBy() {}

  def translateBy() {}

}

object Duration {

}



