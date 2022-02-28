package st4ml.operators.selector

import st4ml.instances._
import st4ml.operators.selector.SelectionUtils._
import st4ml.operators.selector.partitioner.{HashPartitioner, STPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Polygon

import scala.collection.mutable
import scala.reflect.ClassTag

class Selector[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                  tQuery: Duration,
                                                  partitioner: STPartitioner) extends Serializable {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  val parallelism: Int = partitioner.numPartitions

  def loadDf(dataDir: String, metaDataDir: String): DataFrame = {
    val metaData = LoadPartitionInfoLocal(metaDataDir)
    val relatedPartitions = metaData.filter(x =>
      x._2.intersects(sQuery)
        && x._3.intersects(tQuery)
        && x._4 > 0)
      .map(_._1)
    //    println(s"related partitions: ${relatedPartitions.length}")
    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
    if (dirs.length == 0) throw new AssertionError("No data fulfill the ST requirement.")
    //    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
    //    spark.read.parquet(dirs: _*)
    spark.read.parquet(dataDir).filter(col("pId").isin(relatedPartitions: _*))
  }

  def loadDf(dataDir: String): DataFrame = spark.read.parquet(dataDir)


  def selectTraj(dataDir: String, metaDataDir: String = "None", partition: Boolean = true): RDD[Trajectory[Option[String], String]] = {
    import spark.implicits._
    val pInstanceDf = if (metaDataDir == "None") loadDf(dataDir) else loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[T].toRdd
//    val xMin = pInstanceRDD.map(_.extent.xMin).min
//    val yMin = pInstanceRDD.map(_.extent.yMin).min
//    val xMax = pInstanceRDD.map(_.extent.xMax).max
//    val yMax = pInstanceRDD.map(_.extent.yMax).max
//    val tMin = pInstanceRDD.map(_.duration.start).min
//    val tMax = pInstanceRDD.map(_.duration.end).max
//    val area = (xMax - xMin) * (yMax - yMin)
//    println(area / pInstanceRDD.map(_.extent.area).sum / pInstanceRDD.count)
//    println((tMax - tMin) / pInstanceRDD.map(_.duration.seconds).sum / pInstanceRDD.count)
//    println(xMin, yMin, xMax, yMax, tMin, tMax, xMax - xMin, yMax - yMin, tMax - tMin)

    if (partition) pInstanceRDD.filter(_.intersects(sQuery, tQuery)).stPartition(partitioner)
    else pInstanceRDD.filter(_.intersects(sQuery, tQuery))
  }

  def selectEvent(dataDir: String, metaDataDir: String = "None", partition: Boolean = true): RDD[Event[Geometry, Option[String], String]] = {
    import spark.implicits._
    val pInstanceDf = if (metaDataDir == "None") loadDf(dataDir) else loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[E].toRdd
    if (partition) pInstanceRDD.filter(_.intersects(sQuery, tQuery)).stPartition(partitioner)
    else pInstanceRDD.filter(_.intersects(sQuery, tQuery))
  }

  def select(dataDir: String, metaDataDir: String): RDD[I] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[E].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val partitionedRDD = if (pInstanceRDD.getNumPartitions < parallelism)
      pInstanceRDD.stPartition(partitioner)
    else pInstanceRDD
    val rdd1 = partitionedRDD
      .filter(_.intersects(sQuery, tQuery))
      .map(_.asInstanceOf[I])
    rdd1
  }

  def select[A: ClassTag](dataDir: String, metaDataDir: String): RDD[A] = {
    import spark.implicits._
    val pInstanceDf = loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
      case _: String => pInstanceDf.as[E].toRdd
      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
      case _ => throw new ClassCastException("instance type not supported.")
    }
    val partitionedRDD = if (pInstanceRDD.getNumPartitions < parallelism)
      pInstanceRDD.stPartition(partitioner)
    else pInstanceRDD
    val rdd1 = partitionedRDD
      .filter(_.intersects(sQuery, tQuery))
      .map(_.asInstanceOf[A])
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
