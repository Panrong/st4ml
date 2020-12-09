//import java.util.concurrent.atomic.AtomicReference
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.internal.Logging
//import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
//import org.apache.spark.sql.{Encoder, SparkSession}
//import org.apache.spark.sql.catalyst.expressions.Attribute
//
//class STTSession private[stt] (@transient override val sparkContext: SparkContext)
//  extends SparkSession(sparkContext) {self =>
//  @transient
//  private[sql] override lazy val sessionState: SimbaSessionState = {
//    new SimbaSessionState(this)
//  }
//
//  def hasIndex(tableName: String, indexName: String): Boolean = {
//    sessionState.indexManager.lookupIndexedData(table(tableName), indexName).nonEmpty
//  }
//
//  def indexTable(tableName: String, indexType: IndexType,
//                 indexName: String, column: Array[String]): Unit = {
//    val tbl = table(tableName)
//    assert(tbl != null, "Table not found")
//    val attrs = tbl.queryExecution.analyzed.output
//    val columnKeys = column.map(attr => {
//      var ans: Attribute = null
//      for (i <- attrs.indices)
//        if (attrs(i).name.equals(attr)) ans = attrs(i)
//      assert(ans != null, "Attribute not found")
//      ans
//    }).toList
//    sessionState.indexManager.createIndexQuery(table(tableName), indexType,
//      indexName, columnKeys, Some(tableName))
//  }
//
//  def showIndex(tableName: String): Unit = sessionState.indexManager.showQuery(tableName)
//
//  def persistIndex(indexName: String, fileName: String): Unit =
//    sessionState.indexManager.persistIndex(this, indexName, fileName)
//
//  def loadIndex(indexName: String, fileName: String): Unit =
//    sessionState.indexManager.loadIndex(this, indexName, fileName)
//
//  def dropIndexTableByName(tableName: String, indexName: String): Unit = {
//    sessionState.indexManager.dropIndexByNameQuery(table(tableName), indexName)
//  }
//
//  def clearIndex(): Unit = sessionState.indexManager.clearIndex()
//
//  object simbaImplicits extends Serializable {
//    protected[simba] def _simbaContext: SparkSession = self
//
//    implicit def datasetToSimbaDataSet[T : Encoder](ds: SQLDataset[T]): Dataset[T] =
//      Dataset(self, ds.queryExecution.logical)
//
//    implicit def dataframeToSimbaDataFrame(df: SQLDataFrame): DataFrame =
//      Dataset.ofRows(self, df.queryExecution.logical)
//  }
//
//
//}
//
//object STTSession {
//  private val activeThreadSession = new InheritableThreadLocal[STTSession]
//  private val defaultSession = new AtomicReference[STTSession]
//
//  def builder(): Builder = new Builder
//
//  private[sql] def getActiveSession: Option[STTSession] = Option(activeThreadSession.get)
//  private[sql] def getDefaultSession: Option[STTSession] = Option(defaultSession.get)
//
//  def setActiveSession(session: STTSession): Unit = {
//    activeThreadSession.set(session)
//  }
//
//  def clearActiveSession(): Unit = {
//    activeThreadSession.remove()
//  }
//
//  def setDefaultSession(session: STTSession): Unit = {
//    defaultSession.set(session)
//  }
//
//  def clearDefaultSession(): Unit = {
//    defaultSession.set(null)
//  }
//
//
//  class Builder extends Logging {
//
//    private[this] val options = new scala.collection.mutable.HashMap[String, String]
//
//    private[this] var userSuppliedContext: Option[SparkContext] = None
//
//    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
//      userSuppliedContext = Option(sparkContext)
//      this
//    }
//
//    def config(key: String, value: String): Builder = synchronized {
//      options += key -> value
//      this
//    }
//
//    def appName(name: String): Builder = config("spark.app.name", name)
//
//    def master(master: String): Builder = config("spark.master", master)
//
//    def getOrCreate(): STTSession = synchronized {
//      // Get the session from current thread's active session.
//      var session = activeThreadSession.get()
//      if ((session ne null) && !session.sparkContext.isStopped) {
//        options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
//        if (options.nonEmpty) {
//          logWarning("Using an existing SimbaSession; some configuration may not take effect.")
//        }
//        return session
//      }
//
//      // Global synchronization so we will only set the default session once.
//      STTSession.synchronized {
//        // If the current thread does not have an active session, get it from the global session.
//        session = defaultSession.get()
//        if ((session ne null) && !session.sparkContext.isStopped) {
//          options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
//          if (options.nonEmpty) {
//            logWarning("Using an existing SimbaSession; some configuration may not take effect.")
//          }
//          return session
//        }
//
//        // No active nor global default session. Create a new one.
//        val sparkContext = userSuppliedContext.getOrElse {
//          // set app name if not given
//          val randomAppName = java.util.UUID.randomUUID().toString
//          val sparkConf = new SparkConf()
//          options.foreach { case (k, v) => sparkConf.set(k, v) }
//          if (!sparkConf.contains("spark.app.name")) {
//            sparkConf.setAppName(randomAppName)
//          }
//          val sc = SparkContext.getOrCreate(sparkConf)
//          // maybe this is an existing SparkContext, update its SparkConf which maybe used
//          // by SimbaSession
//          options.foreach { case (k, v) => sc.conf.set(k, v) }
//          if (!sc.conf.contains("spark.app.name")) {
//            sc.conf.setAppName(randomAppName)
//          }
//          sc
//        }
//
//        session = new STTSession(sparkContext)
//        options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
//        defaultSession.set(session)
//
//        // Register a successfully instantiated context to the singleton. This should be at the
//        // end of the class definition so that the singleton is updated only if there is no
//        // exception in the construction of the instance.
//        sparkContext.addSparkListener(new SparkListener {
//          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
//            defaultSession.set(null)
//          }
//        })
//      }
//
//      return session
//    }
//  }
//}
