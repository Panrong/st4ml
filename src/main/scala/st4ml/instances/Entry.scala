package st4ml.instances


case class Entry[S <: Geometry, V](spatial: S,
                                   temporal: Duration,
                                   value: V) {

  def extent: Extent = spatial.getEnvelopeInternal

  def duration: Duration = temporal

  def spatialCenter: Point = extent.center

  def temporalCenter: Long = temporal.center

  def spatialCentroid: Point = spatial.getCentroid

  def spatiallyIntersects(geom: Geometry): Boolean = spatial.intersects(geom)

  def temporallyIntersects(dur: Duration): Boolean = temporal.intersects(dur)

  def intersects(geom: Geometry, dur: Duration, how: String = "both"): Boolean = how match {
    case "spatial" => spatiallyIntersects(geom)
    case "temporal" => temporallyIntersects(dur)
    case "both" => spatiallyIntersects(geom) && temporallyIntersects(dur)
    case "either" => spatiallyIntersects(geom) || temporallyIntersects(dur)
    case _ => throw new IllegalArgumentException("Unsupported argument: " +
      s"the third argument could be spatial/temporal/both/either, but got $how")
  }

  def spatiallyContains(geom: Geometry): Boolean = spatial.contains(geom)

  def temporallyContains(dur: Duration): Boolean = temporal.contains(dur)

  def contains(geom: Geometry, dur: Duration, how: String = "both"): Boolean = how match {
    case "spatial" => spatiallyContains(geom)
    case "temporal" => temporallyContains(dur)
    case "both" => spatiallyContains(geom) && temporallyContains(dur)
    case "either" => spatiallyContains(geom) || temporallyContains(dur)
    case _ => throw new IllegalArgumentException("Unsupported argument: " +
      s"the third argument could be spatial/temporal/both/either, but got $how")
  }

}

object Entry {
  def apply[S <: Geometry, V](entry: (S, Duration, V)): Entry[S, V] =
    new Entry(entry._1, entry._2, entry._3)

  def apply[S <: Geometry](entry: (S, Duration)): Entry[S, None.type] =
    new Entry(entry._1, entry._2, None)
}





