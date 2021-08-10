package instances

import scala.reflect.ClassTag

class Raster[S <: Geometry, V, D](
  override val entries: Array[Entry[S, V]],
  override val data: D)
  extends Instance[S, V, D] {

  require(validation,
    s"The length of entries for TimeSeries should be at least 1, but got ${entries.length}")

  override def mapSpatial(f: S => S): Raster[S, V, D] =
    Raster(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  override def mapTemporal(f: Duration => Duration): Raster[S, V, D] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): Raster[S, V1, D] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  override def mapEntries[V1](
    f1: S => S,
    f2: Duration => Duration,
    f3: V => V1): Raster[S, V1, D] =
    Raster(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapData[D1](f: D => D1): Raster[S, V, D1] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  override def toGeometry: Polygon = extent.toPolygon

}

object Raster {
  def empty[T: ClassTag](entryArr: Array[Entry[Polygon, _]]): Raster[Polygon, Array[T], None.type] =
    Raster(entryArr.map(x => Entry(x.spatial, x.temporal, Array.empty[T])))

  def empty[T: ClassTag](extentArr: Array[Extent], durArr: Array[Duration]): Raster[Polygon, Array[T], None.type] = {
    val entryArr = extentArr.zip(durArr).map(x => Entry(x._1.toPolygon, x._2, Array.empty[T]))
    Raster(entryArr)
  }

  def empty[T: ClassTag](polygonArr: Array[Polygon], durArr: Array[Duration]): Raster[Polygon, Array[T], None.type] = {
    val entryArr = polygonArr.zip(durArr).map(x => Entry(x._1, x._2, Array.empty[T]))
    Raster(entryArr)
  }

  def apply[S <: Geometry, V, D](entries: Array[Entry[S, V]], data: D): Raster[S, V, D] =
    new Raster(entries, data)

  def apply[S <: Geometry, V](entries: Array[Entry[S, V]]): Raster[S, V, None.type] =
    new Raster(entries, None)
}