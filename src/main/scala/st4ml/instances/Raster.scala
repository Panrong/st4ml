package st4ml.instances

import scala.reflect.{ClassTag}
import scala.util.control.Breaks._

class Raster[S <: Geometry : ClassTag, V, D](override val entries: Array[Entry[S, V]],
                                             override val data: D)
  extends Instance[S, V, D] {

  require(validation,
    s"The length of entries for TimeSeries should be at least 1, but got ${entries.length}")
  var rTree: Option[RTree[Polygon]] = None

  lazy val temporals: Array[Duration] = entries.map(_.temporal)
  lazy val spatials: Array[S] = entries.map(_.spatial)

  lazy val temporal: Duration = Duration(temporals.head.start, temporals.last.end)

  def isRegular: Boolean = {
    val cubes = entries.map(x => (x.spatial.getEnvelopeInternal.getMinX,
      x.spatial.getEnvelopeInternal.getMinY,
      x.spatial.getEnvelopeInternal.getMaxX,
      x.spatial.getEnvelopeInternal.getMaxY,
      x.temporal.start, x.temporal.end))
    val totalArea = this.extent.area * this.duration.seconds
    val sumArea = cubes.map(x => (x._3 - x._1) * (x._4 - x._2) * (x._6 - x._5)).sum
    val lengths = cubes.map(x => (x._3 - x._1, x._4 - x._2, x._6 - x._5))

    //    def isDisjoint: Boolean = {
    //      if (cubes.length > 1) {
    //        val pairs = cubes.combinations(2)
    //        pairs.foreach(p =>
    //          if (Extent(p(0)._1, p(0)._2, p(0)._3, p(0)._4)
    //            .intersection(Extent(p(1)._1, p(1)._2, p(1)._3, p(1)._4)).isDefined &&
    //            Duration(p(0)._5, p(0)._6).intersection(Duration(p(1)._5, p(1)._6)).isDefined)
    //            return false)
    //      }
    //      true
    //    }

    def overlaps: Boolean = {
      if (cubes.length > 1) {
        val pairs = cubes.combinations(2)
        pairs.foreach(p => {
          val minX1 = p(0)._1
          val minX2 = p(1)._1
          val maxX1 = p(0)._2
          val maxX2 = p(1)._2
          val minY1 = p(0)._3
          val minY2 = p(1)._3
          val maxY1 = p(0)._4
          val maxY2 = p(1)._4
          val minZ1 = p(0)._5
          val minZ2 = p(1)._5
          val maxZ1 = p(0)._6
          val maxZ2 = p(1)._6
          if (((minX1 < minX2 && minX2 < maxX1) || (minX2 < minX1 && minX1 < maxX2)) &&
            ((minY1 < minY2 && minY2 < maxY1) || (minY2 < minY1 && minY1 < maxY2)) &&
            ((minZ1 < minZ2 && minZ2 < maxZ1) || (minZ2 < minZ1 && minZ1 < maxZ2))) {
            //            println(minX1, maxX1, minY1, maxY1, minZ1, maxZ1, minX2, maxX2, minY2, maxY2, minZ2, maxZ2)
            return true
          }
        })
      }
      false
    }

    lengths.forall(x => x == lengths.head) && totalArea == sumArea && !overlaps
  }

  def mapSpatial[T <: Geometry : ClassTag](f: S => T): Raster[T, V, D] =
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
      entries.map(entry => {
        implicit val s: Polygon = entry.spatial.asInstanceOf[Polygon]
        implicit val t: Duration = entry.temporal
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))
      }
      ),
      data)

  def mapValuePlus[V1](f: (V, S, Duration) => V1): Raster[S, V1, D] = {
    val newEntries = entries.map(entry => {
      val spatial = entry.spatial
      val temporal = entry.temporal
      Entry(spatial, temporal, f(entry.value, spatial, temporal))
    })
    Raster(newEntries, data)
  }

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

  def mapEntries[V1](f: Entry[S, V] => Entry[S, V1]): Raster[S, V1, D] =
    Raster(entries.map(f(_)), data)

  override def mapData[D1](f: D => D1): Raster[S, V, D1] =
    Raster(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  def mapDataPlus[D1](f: (D, Polygon, Duration) => D1): Raster[S, V, D1] =
    Raster(entries, f(data, extent.toPolygon, temporal))

  def getEntryIndex[G <: Geometry](
                                    queryArr: Array[(G, Duration)],
                                    how: String
                                  ): Array[Array[Int]] =
    queryArr.map(q =>
      entries
        .zipWithIndex
        .filter(_._1.intersects(q._1, q._2, how))
        .map(_._2)
    )

  def getEntryIndex[G <: Geometry](
                                    queryArr: Array[Array[(G, Duration)]],
                                    how: String
                                  ): Array[Array[Int]] =
    queryArr.map(q =>
      entries
        .zipWithIndex
        .filter(eWithIndex =>
          q.exists(innerTuple => eWithIndex._1.intersects(innerTuple._1, innerTuple._2, how))
        )
        .map(_._2)
    )

  def getEntryIndexRTree[I <: Instance[_, _, _] : ClassTag](queryArr: Array[I]): Array[Array[Int]] =
    queryArr.map(query => rTree.get.range3d(query).map(_._2.toInt))

  def getEntryIndex[G <: Geometry](
                                    geomArr: Array[G],
                                    durArr: Array[Duration],
                                    how: String
                                  ): Array[Array[Int]] = {
    val queryArr = geomArr.zip(durArr)
    getEntryIndex(queryArr, how)
  }

  def getEntryIndex[G <: Geometry](geomArr: Array[G],
                                   timestampArr: Array[Long],
                                   how: String
                                  ): Array[Array[Int]] = {
    val durArr = timestampArr.map(t => Duration(t))
    val queryArr = geomArr.zip(durArr)
    getEntryIndex(queryArr, how)
  }

  def getEntryIndexToObj[T: ClassTag, G <: Geometry](
                                                      objArr: Array[T],
                                                      queryArr: Array[(G, Duration)],
                                                      how: String
                                                    ): Map[Int, Array[T]] = {
    if (queryArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getEntryIndex(queryArr, how)
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

  def getEntryIndexToObj[T: ClassTag, G <: Geometry](
                                                      objArr: Array[T],
                                                      queryArr: Array[Array[(G, Duration)]],
                                                      how: String
                                                    ): Map[Int, Array[T]] = {
    if (queryArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getEntryIndex(queryArr, how)
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

  def getEntryIndexToObjRTree[T <: Instance[_, _, _] : ClassTag, G <: Geometry](
                                                                                 objArr: Array[T],
                                                                                 how: String
                                                                               ): Map[Int, Array[T]] = {
    if (objArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getEntryIndexRTree(objArr)
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

  def createRaster[T: ClassTag](
                                 entryIndexToObj: Map[Int, Array[T]],
                               ): Raster[S, Array[T], D] = {
    if (entryIndexToObj.nonEmpty) {
      val newValues = entries.zipWithIndex.map(entryWithIdx =>
        if (entryIndexToObj.contains(entryWithIdx._2)) {
          entryWithIdx._1.value.asInstanceOf[Array[T]] ++ entryIndexToObj(entryWithIdx._2)
        }
        else {
          entryWithIdx._1.value.asInstanceOf[Array[T]]
        }
      )
      val newEntries = entries.zip(newValues).map(tup =>
        Entry(tup._1.spatial, tup._1.temporal, tup._2)
      )
      Raster(newEntries, data)
    }
    else {
      this.asInstanceOf[Raster[S, Array[T], D]]
    }
  }

  /**
   * the "how" argument could be "spatial", "temporal", "both" or "either"
   * */
  def attachGeometry[T <: Geometry : ClassTag, G <: Geometry : ClassTag](
                                                                          geomArr: Array[T],
                                                                          queryArr: Array[(G, Duration)],
                                                                          how: String = "both"
                                                                        )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    require(geomArr.length == queryArr.length,
      "the length of the first two arguments must match")
    val entryIndexToGeom = getEntryIndexToObj(geomArr, queryArr, how)
    createRaster(entryIndexToGeom)
  }


  /**
   * default: not precise attachment
   *
   * the "how" argument could be "spatial", "temporal", "both" or "either"
   * */
  def attachInstance[T <: Instance[_, _, _] : ClassTag](
                                                         instanceArr: Array[T],
                                                         how: String
                                                       )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    val queryArr = instanceArr.map(x => (x.extent.toPolygon, x.duration))
    val entryIndexToInstance = getEntryIndexToObj(instanceArr, queryArr, how)
    createRaster(entryIndexToInstance)
  }

  /**
   *
   * */
  def attachInstance[T <: Instance[_, _, _] : ClassTag](
                                                         instanceArr: Array[T]
                                                       )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    val queryArr = instanceArr.map(x => (x.toGeometry, x.duration))
    val entryIndexToInstance = getEntryIndexToObj(instanceArr, queryArr, "both")
    createRaster(entryIndexToInstance)
  }

  def attachInstanceRTree[T <: Instance[_, _, _] : ClassTag](
                                                              instanceArr: Array[T]
                                                            )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    val entryIndexToInstance = getEntryIndexToObjRTree(instanceArr, "both")
    createRaster(entryIndexToInstance)
  }

  /**
   * the "how" argument could be "spatial", "temporal", "both" or "either"
   * */
  def attachInstance[T <: Instance[_, _, _] : ClassTag, G <: Geometry : ClassTag](
                                                                                   instanceArr: Array[T],
                                                                                   queryArr: Array[(G, Duration)],
                                                                                   how: String = "both"
                                                                                 )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    require(instanceArr.length == queryArr.length,
      "the length of the first two arguments must match")
    val entryIndexToInstance = getEntryIndexToObj(instanceArr, queryArr, how)
    createRaster(entryIndexToInstance)
  }

  /**
   * A precise version for trajectory
   *
   * the "how" argument could be "spatial", "temporal", "both" or "either"
   * */
  def attachInstanceExact[T <: Instance[_, _, _] : ClassTag, G <: Geometry : ClassTag](
                                                                                        instanceArr: Array[T],
                                                                                        queryArr: Array[Array[(G, Duration)]],
                                                                                        how: String = "both"
                                                                                      )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    require(instanceArr.length == queryArr.length,
      "the length of the first two arguments must match")
    val entryIndexToInstance = getEntryIndexToObj(instanceArr, queryArr, how)
    createRaster(entryIndexToInstance)
  }


  // todo: handle different order of the same spatials
  def merge[T: ClassTag](
                          other: Raster[S, Array[T], _]
                        )(implicit ev: Array[T] =:= V): Raster[S, Array[T], None.type] = {
    val spatials = entries.map(_.spatial)
    val temproals = entries.map(_.temporal)
    require(spatials sameElements other.entries.map(_.spatial),
      "cannot merge Raster with different spatial structure")
    require(temproals sameElements other.entries.map(_.temporal),
      "cannot merge Raster with different temporal structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newEntries = (spatials, temproals, newValues).zipped.toArray.map(Entry(_))
    Raster(newEntries, None)
  }

  def merge[T: ClassTag](
                          other: Raster[S, V, D],
                          valueCombiner: (V, V) => V,
                          dataCombiner: (D, D) => D
                        ): Raster[S, V, D] = {
    val spatials = entries.map(_.spatial)
    val temproals = entries.map(_.temporal)
    require(spatials sameElements other.entries.map(_.spatial),
      "cannot merge Raster with different spatial structure")
    require(temproals sameElements other.entries.map(_.temporal),
      "cannot merge Raster with different temporal structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      valueCombiner(x._1, x._2)
    )
    val newEntries = (spatials, temproals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    Raster(newEntries, newData)
  }

  def merge[T: ClassTag](
                          other: Raster[S, Array[T], D],
                          dataCombiner: (D, D) => D
                        )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = {
    val spatials = entries.map(_.spatial)
    val temproals = entries.map(_.temporal)
    require(spatials sameElements other.entries.map(_.spatial),
      "cannot merge Raster with different spatial structure")
    require(temproals sameElements other.entries.map(_.temporal),
      "cannot merge Raster with different temporal structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newEntries = (spatials, temproals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    Raster(newEntries, newData)
  }

  // sort by tMin then xMin then yMin of the spatial of each entry
  def sorted: Raster[S, V, D] = {
    val newEntries = entries.sortBy(x => (x.temporal.start, x.spatial.getCoordinates.map(_.x).min, x.spatial.getCoordinates.map(_.y).min))
    new Raster[S, V, D](newEntries, data)
  }

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

  def apply[S <: Geometry : ClassTag, V, D](entries: Array[Entry[S, V]], data: D): Raster[S, V, D] =
    new Raster(entries, data)

  def apply[S <: Geometry : ClassTag, V](entries: Array[Entry[S, V]]): Raster[S, V, None.type] =
    new Raster(entries, None)
}