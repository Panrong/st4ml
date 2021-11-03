package instances

import operators.selection.indexer.RTree

import scala.reflect.ClassTag

class SpatialMap[V, D](
  override val entries: Array[Entry[Polygon, V]],
  override val data: D)
  extends Instance[Polygon, V, D] {

  lazy val spatials: Array[Polygon] = entries.map(_.spatial)

  var rTree: Option[RTree[geometry.Rectangle]] = None

  require(validation,
    s"The length of entries for TimeSeries should be at least 1, but got ${entries.length}")

  def isSpatialDisjoint: Boolean = {
    if (spatials.length > 1) {
      val pairs = spatials.combinations(2)
      pairs.foreach(polyPair => if (polyPair(0).intersects(polyPair(1))) return false)
    }
    true
  }

  override def mapSpatial(f: Polygon => Polygon): SpatialMap[V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)


  override def mapTemporal(f: Duration => Duration): SpatialMap[V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): SpatialMap[V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  override def mapEntries[V1](
    f1: Polygon => Polygon,
    f2: Duration => Duration,
    f3: V => V1): SpatialMap[V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapEntries[V1](f: Entry[Polygon, V] => Entry[Polygon, V1]): SpatialMap[V1, D] =
    SpatialMap(entries.map(f(_)), data)

  override def mapData[D1](f: D => D1): SpatialMap[V, D1] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  // todo: test the performance difference: getBinIndices v.s. isSpatialDisjoint + getBinIndex
  def getSpatialIndex[G <: Geometry](geomArr: Array[G]): Array[Array[Int]] = {
    if (geomArr.head.isInstanceOf[Point] && isSpatialDisjoint)
      Utils.getBinIndex(spatials, geomArr)
    else
      Utils.getBinIndices(spatials, geomArr)
  }


  def getSpatialIndexRTree[G <: Geometry](geomArr: Array[G]): Array[Array[Int]] = {
    Utils.getBinIndicesRTree(rTree.get, geomArr)
  }

  def getSpatialIndexToObjRTree[T: ClassTag, G <: Geometry](objArr: Array[T], geomArr: Array[G]): Map[Int, Array[T]] = {
    if (geomArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getSpatialIndexRTree(geomArr)
      objArr.zip(indices)
        .filter(_._2.length > 0)
        .flatMap { case (geom, indexArr) =>
          for {
            idx <- indexArr
          } yield (geom, idx)
        }
        .groupBy(_._2)
        .mapValues(x => x.map(_._1))
    }
  }

  def getSpatialIndexToObj[T: ClassTag, G <: Geometry](objArr: Array[T], geomArr: Array[G]): Map[Int, Array[T]] = {
    if (geomArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getSpatialIndex(geomArr)
      objArr.zip(indices)
        .filter(_._2.length > 0)
        .flatMap { case (geom, indexArr) =>
          for {
            idx <- indexArr
          } yield (geom, idx)
        }
        .groupBy(_._2)
        .mapValues(x => x.map(_._1))
    }
  }

  def createSpatialMap[T: ClassTag](
    spatialIndexToObj: Map[Int, Array[T]],
  ): SpatialMap[Array[T], D] = {
    if (spatialIndexToObj.nonEmpty) {
      val newValues = entries.zipWithIndex.map(entryWithIdx =>
        if (spatialIndexToObj.contains(entryWithIdx._2)) {
          entryWithIdx._1.value.asInstanceOf[Array[T]] ++ spatialIndexToObj(entryWithIdx._2)
        }
        else {
          entryWithIdx._1.value.asInstanceOf[Array[T]]
        }
      )
      val newTemporals = newValues.map (value => Utils.getDuration(value))
      val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
      SpatialMap(newEntries, data)
    }
    else {
      this.asInstanceOf[SpatialMap[Array[T], D]]
    }
  }

  def attachGeometry[T <: Geometry: ClassTag](geomArr: Array[T])
    (implicit ev: Array[T] =:= V): SpatialMap[Array[T], D] = {
    val entryIndexToGeom = getSpatialIndexToObj(geomArr, geomArr)
    createSpatialMap(entryIndexToGeom)
  }

  def attachInstance[T <: Instance[_,_,_] : ClassTag, G <: Geometry: ClassTag]
  (instanceArr: Array[T], geomArr: Array[G])(implicit ev: Array[T] =:= V): SpatialMap[Array[T], D] = {
    require(instanceArr.length == geomArr.length,
      "the length of two arguments must match")
    val entryIndexToInstance = getSpatialIndexToObj(instanceArr, geomArr)
    createSpatialMap(entryIndexToInstance)
  }

  def attachInstanceRTree[T <: Instance[_,_,_] : ClassTag, G <: Geometry: ClassTag]
  (instanceArr: Array[T], geomArr: Array[G])(implicit ev: Array[T] =:= V): SpatialMap[Array[T], D] = {
    require(instanceArr.length == geomArr.length,
      "the length of two arguments must match")
    val entryIndexToInstance = getSpatialIndexToObjRTree(instanceArr, geomArr)
    createSpatialMap(entryIndexToInstance)
  }

  def attachInstance[T <: Instance[_,_,_] : ClassTag](instanceArr: Array[T])
    (implicit ev: Array[T] =:= V): SpatialMap[Array[T], D] = {
    val geomArr = instanceArr.map(_.toGeometry)
    attachInstance(instanceArr, geomArr)
  }


  // todo: handle different order of the same spatials
  def merge[T : ClassTag](
    other: SpatialMap[Array[T], _]
  )(implicit ev: Array[T] =:= V): SpatialMap[Array[T], None.type] = {
    require(spatials sameElements other.spatials,
      "cannot merge SpatialMap with different spatial structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map( x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newTemporals = entries.map(_.temporal).zip(other.entries.map(_.temporal)).map(x =>
      Duration(Array(x._1, x._2))
    )
    val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
    SpatialMap(newEntries, None)
  }

  def merge[T](
    other: SpatialMap[T, D],
    valueCombiner: (V, T) => V,
    dataCombiner: (D, D) => D
  ): SpatialMap[V, D] = {
    require(spatials sameElements other.spatials,
      "cannot merge SpatialMap with different spatial structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map( x =>
      valueCombiner(x._1, x._2)
    )
    val newTemporals = entries.map(_.temporal).zip(other.entries.map(_.temporal)).map(x =>
      Duration(Array(x._1, x._2))
    )
    val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    SpatialMap(newEntries, newData)
  }

  def merge[T : ClassTag](
    other: SpatialMap[Array[T], D],
    dataCombiner: (D, D) => D
  )(implicit ev: Array[T] =:= V): SpatialMap[Array[T], D] = {
    require(spatials sameElements other.spatials,
      "cannot merge SpatialMap with different spatial structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map( x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newTemporals = entries.map(_.temporal).zip(other.entries.map(_.temporal)).map(x =>
      Duration(Array(x._1, x._2))
    )
    val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    SpatialMap(newEntries, newData)
  }

  override def toGeometry: Polygon = extent.toPolygon
}

object SpatialMap {
  def empty[T: ClassTag](polygonArr: Array[Polygon]): SpatialMap[Array[T], None.type] =
    SpatialMap(polygonArr.map(x => Entry(x, Duration.empty, Array.empty[T])))

  def empty[T: ClassTag](extentArr: Array[Extent]): SpatialMap[Array[T], None.type] = {
    val polygonArr = extentArr.map(_.toPolygon)
    SpatialMap.empty(polygonArr)
  }

  def apply[V, D](entries: Array[Entry[Polygon, V]], data: D): SpatialMap[V, D] =
    new SpatialMap(entries, data)

  def apply[V](entries: Array[Entry[Polygon, V]]): SpatialMap[V, None.type] =
    new SpatialMap(entries, None)
}