package instances

abstract class Instance[S <: Geometry, V, D] {

  val entries: Array[Entry[S, V]]
  val data: D

  lazy val extent: Extent =
    Extent(entries.map(_.extent))

  lazy val duration: Duration =
    Duration(entries.map(_.duration))

  def dimension: Int = entries.length

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


  // higher-order types cannot handle cases when S is a specific type, as the signature changes when the return type changes
  def mapSpatial(f: S => S): Instance[S, V, D]
  def mapTemporal(f: Duration => Duration): Instance[S, V, D]
  def mapValue[V1](f: V => V1): Instance[S, V1, D]
  def mapEntries[V1](
    f1: S => S,
    f2: Duration => Duration,
    f3: V => V1): Instance[S, V1, D]
  def mapData[D1](f: D => D1): Instance[S, V, D1]


  override def toString: String =
    this.getClass.getSimpleName +
      s"(entries=${entries.map(_.toString).mkString("Array(", ", ", ")")}, data=${data.toString})"

  override def equals(that: Any): Boolean = {
    that match {
      case that: Instance[S, V, D] =>
        (this.entries sameElements that.entries) &&
          this.data == that.data
      case _ =>
        println("xx")
        false
    }
  }

  override def hashCode(): Int =
    31 * (entries.##) + data.##


}
