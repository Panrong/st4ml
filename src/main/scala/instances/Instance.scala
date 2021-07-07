package instances

trait Instance[S, V, D] {
  val entries: Array[Entry[S, V]]
  val data: D

  lazy val extent: Extent =
    Extent(entries.map(_.extent))

  lazy val duration: Duration =
    Duration(entries.map(_.duration))

  def dimension: Int = entries.size

  def center: (Point, Long) = (spatialCenter, temporalCenter)

  def spatialCenter: Point = extent.center

  def temporalCenter: Long = duration.center


  // Predicates
  // todo: results are not accurate, is it ok?
  def intersects(g: Geometry): Boolean = extent.intersects(g)
  def intersects(e: Extent): Boolean = extent.intersects(e)
  def intersects(dur: Duration): Boolean = duration.intersects(dur)
  def intersects(g: Geometry, dur: Duration): Boolean = intersects(g) && intersects(dur)
  def intersects(e: Extent, dur: Duration): Boolean = intersects(e) && intersects(dur)

  def contains(g: Geometry): Boolean = extent.contains(g)
  def contains(e: Extent): Boolean = extent.contains(e)
  def contains(dur: Duration): Boolean = duration.contains(dur)
  def contains(g: Geometry, dur: Duration): Boolean = contains(g) && contains(dur)
  def contains(e: Extent, dur: Duration): Boolean = contains(e) && contains(dur)

  // Methods
  // todo: test if the modification is in place
//  def mapSpatial[N <: Geometry](f: S => N): Unit =
//    entries.map(entry => Entry(f(entry.spatial), entry.temporal, entry.value))
//
//  def mapTemporal(f: Duration => Duration): Unit =
//    entries.map(entry => Entry(entry.spatial, f(entry.temporal), entry.value))
//
//  def mapValue[N](f: V => N): Unit =
//    entries.map(entry => Entry(entry.spatial, entry.temporal, f(entry.value)))
//
//  def mapData[N](f: D => N): I =


}
