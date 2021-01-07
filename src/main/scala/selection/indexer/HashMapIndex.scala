//package selection.indexer
//
//import org.apache.spark.sql.catalyst.InternalRow
//import java.util.{HashMap => JHashMap}
//
//class HashMapIndex[T] extends Index with Serializable {
//  var selection.indexer = new JHashMap[T, Int]()
//}
//
//object HashMapIndex {
//  def apply[T](dataRDD: Array[(T, InternalRow)]): HashMapIndex[T] = {
//    val res = new HashMapIndex[T]
//    for (i <- dataRDD.indices) {
//      res.selection.indexer.put(dataRDD(i)._1, i)
//    }
//    res
//  }
//}
