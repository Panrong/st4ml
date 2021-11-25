package operatorsNew.selector

import instances.{Duration, Event, Geometry, Instance, Polygon, Trajectory}
import operatorsNew.selector.partitioner.{HashPartitioner, STPartitioner}
import operatorsNew.selector.SelectionUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.reflect.ClassTag

class MultiRangeSelector[I <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                                            tQuery: Array[Duration],
                                                            partitioner: STPartitioner) extends Serializable {
  assert(sQuery.length == tQuery.length, "The lengths of sQuery and tQuery must equal.")
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  type SttEvent = Event[Geometry, Option[String], String]

  type SttTraj = Trajectory[Option[String], String]

  def loadDf(dataDir: String, metaDataDir: String): DataFrame = {
    val totalSRange = sQuery.foldRight(Polygon.empty)(_.union(_).getEnvelope.asInstanceOf[Polygon])
    val totalTRange = Duration(tQuery.map(_.start).min, tQuery.map(_.end).max)
    val metaData = LoadPartitionInfoLocal(metaDataDir)
    val relatedPartitions = metaData.filter(x =>
      x._2.intersects(totalSRange)
        && x._3.intersects(totalTRange)
        && x._4 > 0)
      .map(_._1)
//    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
//    spark.read.parquet(dirs: _*)
    spark.read.parquet(dataDir).filter(col("pId").isin(relatedPartitions:_*))
  }

  def selectEvent(dataDir: String, metaDataDir: String): RDD[SttEvent] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[E].toRdd
    val queries = sQuery zip tQuery
    pInstanceRDD.stPartition(partitioner)
      .filter(o =>
        queries.exists(q => o.intersects(q._1, q._2)))
  }

  def selectEventWithInfo(dataDir: String,
                          metaDataDir: String,
                          accurate: Boolean = false): RDD[(SttEvent, Array[Int])] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[E].toRdd
    val queries = (sQuery zip tQuery).zipWithIndex
    pInstanceRDD.stPartition(partitioner)
      .map(o => {
        lazy val geom = o.toGeometry
        val intersections = if (accurate)
          queries.filter(q =>
            geom.intersects(q._1._1) && o.intersects(q._1._2))
            .map(_._2)
        else queries.filter(q =>
          o.intersects(q._1._1, q._1._2))
          .map(_._2)
        (o, intersections)
      })
      .filter(!_._2.isEmpty)
  }

  def selectTraj(dataDir: String, metaDataDir: String): RDD[SttTraj] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[T].toRdd
    val queries = sQuery zip tQuery
    pInstanceRDD.stPartition(partitioner)
      .filter(o =>
        queries.exists(q => o.intersects(q._1, q._2)))
  }

  def selectTrajWithInfo(dataDir: String,
                         metaDataDir: String,
                         accurate: Boolean = false): RDD[(SttTraj, Array[Int])] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[T].toRdd
    val queries = (sQuery zip tQuery).zipWithIndex
    pInstanceRDD.stPartition(partitioner)
      .map(o => {
        lazy val geom = o.toGeometry
        val intersections = if (accurate)
          queries.filter(q =>
            geom.intersects(q._1._1) && o.intersects(q._1._2))
            .map(_._2)
        else queries.filter(q =>
          o.intersects(q._1._1, q._1._2))
          .map(_._2)
        (o, intersections)
      })
      .filter(!_._2.isEmpty)
  }

  def select(dataDir: String, metaDataDir: String): RDD[I] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[E].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val queries = sQuery zip tQuery
    pInstanceRDD.stPartition(partitioner)
      .filter(o =>
        queries.exists(q => o.intersects(q._1, q._2)))
      .map(_.asInstanceOf[I])
  }

  def selectWithInfo(dataDir: String,
                     metaDataDir: String,
                     accurate: Boolean = false): RDD[(I, Array[Int])] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[E].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val queries = (sQuery zip tQuery).zipWithIndex
    pInstanceRDD.stPartition(partitioner)
      .map(o => {
        lazy val geom = o.toGeometry
        val intersections = if (accurate)
          queries.filter(q =>
            geom.intersects(q._1._1) && o.intersects(q._1._2))
            .map(_._2)
        else queries.filter(q =>
          o.intersects(q._1._1, q._1._2))
          .map(_._2)
        (o, intersections)
      })
      .filter(!_._2.isEmpty)
      .map(_.asInstanceOf[(I, Array[Int])])
  }
}

object MultiRangeSelector {
  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                               tQuery: Array[Duration],
                                               partitioner: STPartitioner): MultiRangeSelector[I] =
    new MultiRangeSelector[I](sQuery, tQuery, partitioner)

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                               tQuery: Array[Duration],
                                               numPartitions: Int): MultiRangeSelector[I] =
    new MultiRangeSelector[I](sQuery, tQuery, new HashPartitioner(numPartitions))

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                               tQuery: Duration,
                                               partitioner: STPartitioner): MultiRangeSelector[I] =
    new MultiRangeSelector[I](sQuery, Array.fill(sQuery.length)(tQuery), partitioner)

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Array[Duration],
                                               partitioner: STPartitioner): MultiRangeSelector[I] =
    new MultiRangeSelector[I](Array.fill(tQuery.length)(sQuery), tQuery, partitioner)

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                               tQuery: Duration,
                                               numPartitions: Int): MultiRangeSelector[I] =
    new MultiRangeSelector[I](sQuery, Array.fill(sQuery.length)(tQuery), new HashPartitioner(numPartitions))

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Array[Duration],
                                               numPartitions: Int): MultiRangeSelector[I] =
    new MultiRangeSelector[I](Array.fill(tQuery.length)(sQuery), tQuery, new HashPartitioner(numPartitions))
}