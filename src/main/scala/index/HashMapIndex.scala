package index

import org.apache.spark.sql.catalyst.InternalRow
import java.util.{HashMap => JHashMap}

class HashMapIndex[T] extends Index with Serializable {
  var index = new JHashMap[T, Int]()
}

object HashMapIndex {
  def apply[T](data: Array[(T, InternalRow)]): HashMapIndex[T] = {
    val res = new HashMapIndex[T]
    for (i <- data.indices) {
      res.index.put(data(i)._1, i)
    }
    res
  }
}
