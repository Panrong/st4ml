package index

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan


import _root_.STTSession

private[stt] case class IPartition(data: Array[InternalRow], index: Index)

private[stt] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>
  var _indexedRDD: IndexedRDD
  def indexedRDD: IndexedRDD = _indexedRDD

  def simbaSession = STTSession.getActiveSession.orNull

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}

private[stt] object IndexedRelation {
  def apply(child: SparkPlan, table_name: Option[String], index_type: IndexType,
            column_keys: List[Attribute], index_name: String): IndexedRelation = {
    index_type match {
      case RTreeType =>
        RTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case HashMapType =>
        HashMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case _ => null
    }
  }
}
