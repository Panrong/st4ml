package index

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.NumericType

import partitioner.STRPartitioner

private[stt] case class RTreeIndexedRelation (
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: IndexedRDD = null, var global_rtree: RTree = null)
  extends IndexedRelation with MultiInstanceRelation{

  var isPoint = false

  private def checkKeys: Boolean = {
    if (column_keys.length > 1) {
      for (i <- column_keys.indices)
        if (!column_keys(i).dataType.isInstanceOf[NumericType]) {
          return false
        }
      true
    } else { // length = 1; we do not support one dimension R-tree
      column_keys.head.dataType match {
        case t: ShapeType =>
          isPoint = true
          true
        case _ => false
      }
    }
  }
  require(checkKeys)

  val dimension = ShapeUtils.getPointFromRow(child.execute().first(), column_keys, child, isPoint).coord.length

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[stt] def buildIndex(): Unit = {
    val numShufflePartitions = simbaSession.sessionState.sttConf.indexPartitions
    val maxEntriesPerNode = simbaSession.sessionState.sttConf.maxEntriesPerNode
    val sampleRate = simbaSession.sessionState.sttConf.sampleRate
    val transferThreshold = simbaSession.sessionState.sttConf.transferThreshold
    val dataRDD = child.execute().map(row => {
      (ShapeUtils.getPointFromRow(row, column_keys, child, isPoint), row)
    })

    val max_entries_per_node = maxEntriesPerNode
    val (partitionedRDD, mbr_bounds) =
      STRPartitioner(dataRDD, dimension, numShufflePartitions, sampleRate, transferThreshold, max_entries_per_node)

    val indexed = partitionedRDD.mapPartitions { iter =>
      val data = iter.toArray
      var index: RTree = null
      if (data.length > 0) index = RTree(data.map(_._1).zipWithIndex, max_entries_per_node)
      Array(IPartition(data.map(_._2), index)).iterator
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitionSize = indexed.mapPartitions(iter => iter.map(_.data.length)).collect()

    global_rtree = RTree(mbr_bounds.zip(partitionSize)
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)
    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    RTreeIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    RTreeIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, global_rtree)
  }
}
