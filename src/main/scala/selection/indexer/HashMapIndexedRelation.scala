//package selection.indexer
//
//import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
//import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
//import org.apache.spark.sql.execution.SparkPlan
//import org.apache.spark.sql.types.NumericType
//import org.apache.spark.storage.StorageLevel
//import partitioner.HashPartition
//
//
//private[stt] case class HashMapIndexedRelation(output: Seq[Attribute], child: SparkPlan,
//                                                 table_name: Option[String], column_keys: List[Attribute], index_name: String)(var _indexedRDD: IndexedRDD = null)
//  extends IndexedRelation with MultiInstanceRelation {
//
//  require(column_keys.length == 1)
//  require(column_keys.head.dataType.isInstanceOf[NumericType])
//  if (_indexedRDD == null) {
//    buildIndex()
//  }
//
//  private[simba] def buildIndex(): Unit = {
//    val numShufflePartitions = simbaSession.sessionState.sttConf.indexPartitions
//
//    val dataRDD = child.execute().map(row => {
//      val eval_key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
//      (eval_key, row)
//    })
//
//    val partitionedRDD = HashPartition(dataRDD, numShufflePartitions)
//    val indexed = partitionedRDD.mapPartitions(iter => {
//      val dataRDD = iter.toArray
//      val selection.indexer = HashMapIndex(dataRDD)
//      Array(IPartition(dataRDD.map(_._2), selection.indexer)).iterator
//    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
//    _indexedRDD = indexed
//  }
//
//  override def newInstance(): IndexedRelation = {
//    HashMapIndexedRelation(output.map(_.newInstance()), child, table_name,
//      column_keys, index_name)(_indexedRDD).asInstanceOf[this.type]
//  }
//
//  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
//    HashMapIndexedRelation(new_output, child, table_name, column_keys, index_name)(_indexedRDD)
//  }
//}
