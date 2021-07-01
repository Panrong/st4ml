package instances

import java.time.{Instant => jInstant}

object Instant extends InstantConstructors

trait InstantConstructors {
  def apply(t: Long): Instant = jInstant.ofEpochSecond(t)

  def apply(t: String): Instant = jInstant.parse(t)
}

