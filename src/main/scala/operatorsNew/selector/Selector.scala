package operatorsNew.selector

import instances._
import operatorsNew.selector.SelectionUtils._
import operatorsNew.selector.partitioner.{HashPartitioner, STPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Polygon

import java.lang.System.nanoTime
import scala.collection.mutable
import scala.reflect.ClassTag

class Selector[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                  tQuery: Duration,
                                                  partitioner: STPartitioner) extends Serializable {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  def loadDf(dataDir: String, metaDataDir: String): DataFrame = {
    val t = nanoTime
//    val metaData = LoadPartitionInfo(metaDataDir).collect
    val metaData = LoadPartitionInfoLocal(metaDataDir)
    println(s"redundancy ${metaData.map(_._4).sum}")
    val relatedPartitions = metaData.filter(x =>
      x._2.intersects(sQuery)
        && x._3.intersects(tQuery)
        && x._4 > 0)
      .map(_._1)
    println(s"related partitions: ${relatedPartitions.length}")
    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
//    println(s"get related partitions: ${(nanoTime-t) * 1e-9}")
    if (dirs.length == 0) throw new AssertionError("No data fulfill the ST requirement.")
    //    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
    //    spark.read.parquet(dirs: _*)
    spark.read.parquet(dataDir).filter(col("pId").isin(relatedPartitions:_*))
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
    var t = nanoTime()
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[E].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val partitionedRDD = pInstanceRDD.stPartition(partitioner)
    //    println(s"metadata: ${pInstanceRDD.count}")
    partitionedRDD.count
    println(s"data loading：${(nanoTime() - t) * 1e-9} s.")
    t = nanoTime()
    val rdd1 = partitionedRDD
      .filter(_.intersects(sQuery, tQuery))
      .map(_.asInstanceOf[I])
    println(rdd1.count)
    println(s"metadata selection time: ${(nanoTime() - t) * 1e-9} s.")
    rdd1
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
                                               partitioner: STPartitioner): Selector[I] = {
    new Selector[I](sQuery, tQuery, partitioner)
  }
}
