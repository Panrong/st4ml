package st4ml.instances

import scala.reflect.ClassTag

abstract class Instance[S <: Geometry, V, D] extends Serializable {

  val entries: Array[Entry[S, V]]
  val data: D

  def validation: Boolean = entries.length > 0

  lazy val extent: Extent =
    Extent(entries.map(_.extent))

  lazy val duration: Duration =
    Duration(entries.map(_.duration))

  def entryLength: Int = entries.length

  def center: (Point, Long) = (spatialCenter, temporalCenter)

  def spatialCenter: Point = extent.center

  def temporalCenter: Long = duration.center

  def toGeometry: Geometry

  // Predicates
  // modified from extent to actual geometry
  def intersects(g: Geometry): Boolean = this.toGeometry.intersects(g)

  def intersects(e: Extent): Boolean = this.toGeometry.intersects(e)

  def intersects(dur: Duration): Boolean = duration.intersects(dur)

  def intersects(g: Geometry, dur: Duration): Boolean = intersects(g) && intersects(dur)

  def intersects(e: Extent, dur: Duration): Boolean = intersects(e) && intersects(dur)

  def contains(g: Geometry): Boolean = this.toGeometry.contains(g)

  def contains(e: Extent): Boolean = this.toGeometry.contains(e)

  def contains(dur: Duration): Boolean = duration.contains(dur)

  def contains(g: Geometry, dur: Duration): Boolean = contains(g) && contains(dur)

  def contains(e: Extent, dur: Duration): Boolean = contains(e) && contains(dur)

  // Methods
  //  def mapSpatial[T <: Geometry : ClassTag, G <: Instance[_, _, _]](f: S => T): G

  def mapTemporal(f: Duration => Duration): Instance[S, V, D]

  def mapValue[V1](f: V => V1): Instance[S, V1, D]

  def mapEntries[V1](f1: S => S,
                     f2: Duration => Duration,
                     f3: V => V1): Instance[S, V1, D]

  def mapEntries[V1](f: Entry[S, V] => Entry[S, V1]): Instance[S, V1, D]

  def mapData[D1](f: D => D1): Instance[S, V, D1]

  def isEmpty: Boolean =
    entries.length == 0 || entries.map(_.spatial.isEmpty).forall(_ == true)


  override def toString: String =
    this.getClass.getSimpleName +
      s"(entries=${entries.map(_.toString).mkString("Array(", ", ", ")")}, data=${data.toString})"

  override def equals(that: Any): Boolean = {
    that match {
      case that: Instance[S, V, D] =>
        (this.entries sameElements that.entries) &&
          this.data == that.data
      case _ => false
    }
  }

  override def hashCode(): Int =
    31 * entries.## + data.##

}



