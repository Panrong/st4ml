package operatorsNew.selector

import instances._
import operatorsNew.selector.SelectionUtils._
import operatorsNew.selector.partitioner.{HashPartitioner, SpatialPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.locationtech.jts.geom.Polygon

import scala.collection.mutable
import scala.reflect.ClassTag

class Selector[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                  tQuery: Duration,
                                                  partitioner: SpatialPartitioner) extends Serializable with Ss {

  def loadDf(dataDir: String, metaDataDir: String): DataFrame = {
    val metaData = LoadMetadata(metaDataDir)
    val relatedPartitions = metaData.filter(x =>
      x._2.intersects(sQuery)
        && x._3.intersects(tQuery)
        && x._4 > 0)
      .map(_._1)
    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
    spark.read.parquet(dirs: _*)
  }

  def selectTraj(dataDir: String, metaDataDir: String): RDD[Trajectory[Option[String], String]] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[T].toRdd
    pInstanceRDD.stPartition(partitioner)
      .filter(_.intersects(sQuery, tQuery))
  }

  def selectEvent(dataDir: String, metaDataDir: String): RDD[Event[Geometry, Option[String], String]] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[E].toRdd
    pInstanceRDD.stPartition(partitioner)
      .filter(_.intersects(sQuery, tQuery))
  }

  def select(dataDir: String, metaDataDir: String): RDD[I] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[E].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    pInstanceRDD.stPartition(partitioner)
      .filter(_.intersects(sQuery, tQuery))
      .map(_.asInstanceOf[I])
  }
}

object Selector {
  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Duration,
                                               numPartitions: Int): Selector[I] = {
    new Selector[I](sQuery, tQuery, new HashPartitioner(numPartitions))
  }

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Duration,
                                               partitioner: SpatialPartitioner): Selector[I] = {
    new Selector[I](sQuery, tQuery, partitioner)
  }
}
