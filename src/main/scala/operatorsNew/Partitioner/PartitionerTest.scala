package operatorsNew.Partitioner

import org.apache.spark.sql.SparkSession
import preprocessing.ParquetReader

object PartitionerTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("partitionerTest")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val numPartitions = 16

    //    val eventRDD = ParquetReader.readFaceParquet("datasets/face_example.parquet")
    //    println(s"Total number of events ${eventRDD.count}")
    //    println(s"Example: ${eventRDD.take(1).deep}")
    //    println("=========================")
    //    println("Hash Partitioner:")
    //    val partitioner1 = new HashPartitioner(numPartitions)
    //    val partitionedRDD1 = partitioner1.partition(eventRDD)
    //    println(s"number of partitions: ${partitionedRDD1.getNumPartitions}")
    //    println("number of events in each partition:")
    //    println(partitionedRDD1.mapPartitions(p => Iterator(p.size)).collect.deep)
    //    println(s"Total events: ${partitionedRDD1.count}")
    //
    //    println("=========================")
    //    println("STR Partitioner:")
    //    val partitioner2 = new STRPartitioner(numPartitions)
    //    val partitionedRDD2 = partitioner2.partition(eventRDD)
    //    println(s"number of partitions: ${partitionedRDD2.getNumPartitions}")
    //    println("number of events in each partition:")
    //    println(partitionedRDD2.mapPartitions(p => Iterator(p.size)).collect.deep)
    //    println(s"Total events: ${partitionedRDD2.count}")

    val trajRDD = ParquetReader.readVhcParquet("datasets/traj_example.parquet")
    println(s"Total number of events ${trajRDD.count}")
    println(s"Example: ${trajRDD.take(1).deep}")

    println("=========================")
    println("Hash Partitioner:")
    val partitioner3 = new HashPartitioner(numPartitions)
    val partitionedRDD3 = partitioner3.partition(trajRDD)
    println(s"number of partitions: ${partitionedRDD3.getNumPartitions}")
    println("number of trajectories in each partition:")
    println(partitionedRDD3.mapPartitions(p => Iterator(p.size)).collect.deep)
    println(s"Total trajectories: ${partitionedRDD3.count}")

    println("=========================")
    println("STR Partitioner:")
    val partitioner4 = new STRPartitioner(numPartitions)
    val partitionedRDD4 = partitioner4.partition(trajRDD)
    println(s"number of partitions: ${partitionedRDD4.getNumPartitions}")
    println("number of trajectories in each partition:")
    println(partitionedRDD4.mapPartitions(p => Iterator(p.size)).collect.deep)
    println(s"Total trajectories: ${partitionedRDD4.count}")

    println("=========================")
    println("STR Partitioner with duplication:")
    val partitioner5 = new STRPartitioner(numPartitions)
    val partitionedRDD5 = partitioner5.partitionWDup(trajRDD)
    println(s"number of partitions: ${partitionedRDD5.getNumPartitions}")
    println("number of trajectories in each partition:")
    println(partitionedRDD5.mapPartitions(p => Iterator(p.size)).collect.deep)
    println(s"Total trajectories: ${partitionedRDD5.count}")

    sc.stop()
  }
}
