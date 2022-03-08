package st4ml.operators.selector

import st4ml.instances._
import st4ml.operators.selector.SelectionUtils._
import st4ml.operators.selector.partitioner.{HashPartitioner, STPartitioner}
import st4ml.operators.Operator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.reflect.ClassTag

class Selector[I <: Instance[_, _, _] : ClassTag](var sQuery: Polygon = Polygon.empty,
                                                  var tQuery: Duration = Duration.empty,
                                                  var parallelism: Int = 1,
                                                 ) extends Operator with Serializable {

  var partitioner: STPartitioner = new HashPartitioner(parallelism)
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  def setSQuery(p: Polygon): this.type = {
    sQuery = p
    this
  }

  def setTQuery(d: Duration): this.type = {
    tQuery = d
    this
  }

  // Since the data loaded into memory (from HDFS or S3) are already roughly balance
  // the default Hash partitioner is removed
  def setPartitioner(p: STPartitioner): this.type = {
    partitioner = p
    this
  }

  def setParallelism(p: Int): this.type = {
    parallelism = p
    this
  }

  def loadDf(dataDir: String, metaDataDir: String): DataFrame = {
    val metaData = LoadPartitionInfoLocal(metaDataDir)
    val relatedPartitions = metaData.filter(x =>
      x._2.intersects(sQuery)
        && x._3.intersects(tQuery)
        && x._4 > 0)
      .map(_._1)
    val dirs = relatedPartitions.map(x => dataDir + s"/pId=$x")
    if (dirs.length == 0) throw new AssertionError("No data fulfill the ST requirement.")
    spark.read.parquet(dataDir).filter(col("pId").isin(relatedPartitions: _*))
  }

  def loadDf(dataDir: String): DataFrame = spark.read.parquet(dataDir)

  def selectTraj(dataDir: String, metaDataDir: String = "None", partition: Boolean = true): RDD[Trajectory[Option[String], String]] = {
    val pInstanceDf = if (metaDataDir == "None") loadDf(dataDir) else loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[T].toRdd
    if (partition) pInstanceRDD.filter(_.intersects(sQuery, tQuery)).stPartition(partitioner)
    else pInstanceRDD.filter(_.intersects(sQuery, tQuery))
  }

  def selectEvent(dataDir: String, metaDataDir: String = "None", partition: Boolean = true): RDD[Event[Geometry, Option[String], String]] = {
    val pInstanceDf = if (metaDataDir == "None") loadDf(dataDir) else loadDf(dataDir, metaDataDir)
    val pInstanceRDD = pInstanceDf.as[E].toRdd
    if (partition) pInstanceRDD.filter(_.intersects(sQuery, tQuery)).stPartition(partitioner)
    else pInstanceRDD.filter(_.intersects(sQuery, tQuery))
  }

  def select(dataDir: String, metaDataDir: String): RDD[I] = {
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

  //  def select[A: ClassTag](dataDir: String, metaDataDir: String): RDD[A] = {
  //    val pInstanceDf = loadDf(dataDir, metaDataDir)
  //    val pInstanceRDD = pInstanceDf.head(1).head.get(0) match {
  //      case _: String => pInstanceDf.as[E].toRdd
  //      case _: mutable.WrappedArray[_] => pInstanceDf.as[T].toRdd
  //      case _ => throw new ClassCastException("instance type not supported.")
  //    }
  //    val partitionedRDD = if (pInstanceRDD.getNumPartitions < parallelism)
  //      pInstanceRDD.stPartition(partitioner.get)
  //    else pInstanceRDD
  //    val rdd1 = partitionedRDD
  //      .filter(_.intersects(sQuery, tQuery))
  //      .map(_.asInstanceOf[A])
  //    rdd1
  //  }
}

// legacy
object Selector {
  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Duration,
                                               numPartitions: Int): Selector[I] = {
    new Selector[I](sQuery, tQuery, numPartitions)
  }

  def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                               tQuery: Duration,
                                               partitioner: STPartitioner): Selector[I] = {
    new Selector[I](sQuery, tQuery).setPartitioner(partitioner)
  }
}
