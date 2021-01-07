//import java.util.concurrent.locks.ReentrantReadWriteLock
//
//import org.apache.spark.internal.Logging
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.Dataset
//import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import selection.indexer._
//import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
//
//import scala.collection.mutable.ArrayBuffer
//
//private case class IndexedData(name: String, plan: LogicalPlan, indexedData: IndexedRelation)
//
//case class IndexInfo(tableName: String, indexName: String,
//                     attributes: Seq[Attribute], indexType: IndexType,
//                     location: String, storageLevel: StorageLevel) extends Serializable
//
//private[stt] class IndexManager extends Logging {
//  @transient
//  private val indexedData = new ArrayBuffer[IndexedData]
//
//  @transient
//  private val indexLock = new ReentrantReadWriteLock
//
//  @transient
//  private val indexInfos = new ArrayBuffer[IndexInfo]
//
//  def getIndexInfo: Array[IndexInfo] = indexInfos.toArray
//
//  private def readLock[A](f: => A): A = {
//    val lock = indexLock.readLock()
//    lock.lock()
//    try f finally {
//      lock.unlock()
//    }
//  }
//
//  private def writeLock[A](f: => A): A = {
//    val lock = indexLock.writeLock()
//    lock.lock()
//    try f finally {
//      lock.unlock()
//    }
//  }
//
//  private[stt] def isEmpty: Boolean = readLock {
//    indexedData.isEmpty
//  }
//
//  private[stt] def lookupIndexedData(query: Dataset[_]): Option[IndexedData] = readLock {
//    ???
//  }
//
//  private[stt] def lookupIndexedData(query: Dataset[_], indexName: String): Option[IndexedData] = readLock {
//    ???
//  }
//
//  private[stt] def lookupIndexedData(plan: LogicalPlan): Option[IndexedData] = readLock {
//    ???
//  }
//
//  private[stt] def lookupIndexedData(plan: LogicalPlan, indexName: String): Option[IndexedData] = readLock {
//    ???
//  }
//
//  private[stt] def persistIndex(sttSession: STTSession, indexName: String, fileName: String): Unit = ???
//
//  private[stt] def loadIndex(sttSession: STTSession, indexName: String, fileName: String): Unit = ???
//
//  private[stt] def setStorageLevel(query: Dataset[_], indexName: String, newLevel: StorageLevel): Unit = writeLock {
//    ???
//  }
//
//  private[stt] def createIndexQuery(
//    query: Dataset[_],
//    indexType: IndexType,
//    indexName: String,
//    column: List[Attribute],
//    tableName: Option[String] = None,
//    storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
//    ???
//  }
//
//  private[stt] def showQuery(tableName: String): Unit = readLock {
//    indexInfos.map(row => {
//      if (row.tableName.equals(tableName)) {
//        println("Index " + row.indexName + " {")
//        println("\tTable: " + tableName)
//        print("\tOn column: (")
//        for (i <- row.attributes.indices)
//          if (i != row.attributes.length - 1) {
//            print(row.attributes(i).name + ", ")
//          } else println(row.attributes(i).name + ")")
//        println("\tIndex Type: " + row.indexType.toString)
//        println("}")
//      }
//      row
//    })
//  }
//
//  private[stt] def dropIndexQuery(query: Dataset[_], blocking: Boolean = true): Unit = writeLock {
//    ???
//  }
//
//  private[stt] def dropIndexByNameQuery(query: Dataset[_], indexName: String, blocking: Boolean = true): Unit = writeLock {
//    ???
//  }
//
//  private[stt] def tryDropIndexQuery(query: Dataset[_], blocking: Boolean = true): Boolean = writeLock {
//    ???
//  }
//
//  private[stt] def tryDropIndexByNameQuery(query: Dataset[_], indexName: String, blocking: Boolean = true): Boolean = writeLock {
//    ???
//  }
//
//  private[stt] def clearIndex(): Unit = writeLock {
//    indexedData.foreach(_.indexedData.indexedRDD.unpersist())
//    indexedData.clear()
//    indexInfos.clear()
//  }
//
//  private[stt] def useIndexedData(plan: LogicalPlan): LogicalPlan = {
//    plan transformDown {
//      case currentFragment =>
//        lookupIndexedData(currentFragment)
//          .map(_.indexedData.withOutput(currentFragment.output))
//          .getOrElse(currentFragment)
//    }
//  }
//
//
//}
