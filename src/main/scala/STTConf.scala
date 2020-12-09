//import java.util.{Properties, HashMap => JHashMap}
//import java.util.Collections.{synchronizedMap => JSysnchronizedMap}
//
//import scala.collection.JavaConverters._
//
//private[stt] class STTConf extends Serializable {
//  import STTConf._
//
//  @transient
//  protected[stt] val settings = JSysnchronizedMap(new JHashMap[String, String]())
//
//  private[stt] def maxEntriesPerNode: Int = getConf(MAX_ENTRIES_PER_NODE)
//
//  def setConf(props: Properties): Unit = settings.synchronized {
//    props.asScala.foreach { case (k, v) => setConfString(k, v) }
//  }
//
//  def setConf[T](entry: STTConfEntry[T], value: T): Unit = {
//    require(entry != null, "entry cannot be null")
//    require(value != null, s"value cannot be null for key: ${entry.key}")
//    require(registeredConfEntries.get(entry.key) == entry, s"$entry is not registered")
//    settings.put(entry.key, entry.stringConverter(value))
//  }
//
//  def getConf[T](entry: STTConfEntry[T], defaultValue: T): T = {
//    require(registeredConfEntries.get(entry.key) == entry, s"$entry is not registered")
//    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
//  }
//
//  def getConf[T](entry: STTConfEntry[T]): T = {
//    require(registeredConfEntries.get(entry.key) == entry, s"$entry is not registered")
//    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
//      getOrElse(throw new NoSuchElementException(entry.key))
//  }
//
//  def setConfString(key: String, value: String): Unit = {
//    require(key != null, "key cannot be null")
//    require(value != null, s"value cannot be null for key: $key")
//    val entry = registeredConfEntries.get(key)
//    if (entry != null) {
//      entry.valueConverter(value)
//    }
//    settings.put(key, value)
//  }
//
//  def getConfString(key: String, defaultValue: String): String = {
//    val entry = registeredConfEntries.get(key)
//    if (entry != null && defaultValue != "<undefined>") {
//      entry.valueConverter(defaultValue)
//    }
//    Option(settings.get(key)).getOrElse(defaultValue)
//  }
//
//  def getAllConf: Map[String, String] = settings.synchronized{
//    settings.asScala.toMap
//  }
//
//  def getAllRegisteredConf: Seq[(String, String, String)] = registeredConfEntries.synchronized{
//    registeredConfEntries.values.asScala.filter(_.isPublic).map { entry =>
//      (entry.key, entry.defaultValueString, entry.doc)
//    }.toSeq
//  }
//
//  private[stt] def unsetConf(key: String): Unit = {
//    settings.remove(key)
//  }
//
//  private[stt] def unsetConf(entry: STTConfEntry[_]): Unit = {
//    settings.remove(entry.key)
//  }
//
//  private[stt] def clearConf(): Unit = {
//    settings.clear()
//  }
//
//}
//
//private[stt] object STTConf {
//  private val registeredConfEntries = JSysnchronizedMap(new JHashMap[String, STTConfEntry[_]]())
//
//  // RTree parameters
//  val MAX_ENTRIES_PER_NODE: STTConfEntry[Int] = STTConfEntry.intConf("stt.rtree.maxEntriesPerNode", Some(25))
//
//  private[stt] class STTConfEntry[T] (
//                                       val key: String,
//                                       val defaultValue: Option[T],
//                                       val valueConverter: String => T,
//                                       val stringConverter: T => String,
//                                       val doc: String,
//                                       val isPublic: Boolean) {
//
//    def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")
//
//    override def toString: String = {
//      s"STTConfEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, isPublic=$isPublic)"
//    }
//
//  }
//
//  private[stt] object STTConfEntry {
//    private def apply[T](
//                          key: String,
//                          defaultValue: Option[T],
//                          valueConverter: String => T,
//                          stringConverter: T => String,
//                          doc: String,
//                          isPublic: Boolean): STTConfEntry[T] = registeredConfEntries.synchronized {
//      if (registeredConfEntries.containsKey(key)) {
//        throw new IllegalArgumentException(s"Duplicate STTConfEntry. $key has been registered")
//      }
//      val entry = new STTConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
//      registeredConfEntries.put(key, entry)
//      entry
//    }
//
//    def intConf(key: String, defaultValue: Option[Int] = None,
//                doc: String = "", isPublic: Boolean = true): STTConfEntry[Int] =
//      STTConfEntry(key, defaultValue, { v =>
//        try {
//          v.toInt
//        } catch {
//          case _: NumberFormatException =>
//            throw new IllegalArgumentException(s"$key should be int, but was $v")
//        }
//      }, _.toString, doc, isPublic)
//
//    def longConf(key: String, defaultValue: Option[Long] = None,
//                 doc: String = "", isPublic: Boolean = true): STTConfEntry[Long] =
//      STTConfEntry(key, defaultValue, { v =>
//        try {
//          v.toLong
//        } catch {
//          case _: NumberFormatException =>
//            throw new IllegalArgumentException(s"$key should be long, but was $v")
//        }
//      }, _.toString, doc, isPublic)
//
//    def doubleConf(key: String, defaultValue: Option[Double] = None,
//                   doc: String = "", isPublic: Boolean = true): STTConfEntry[Double] =
//      STTConfEntry(key, defaultValue, { v =>
//        try {
//          v.toDouble
//        } catch {
//          case _: NumberFormatException =>
//            throw new IllegalArgumentException(s"$key should be double, but was $v")
//        }
//      }, _.toString, doc, isPublic)
//
//    def booleanConf(key: String, defaultValue: Option[Boolean] = None,
//                    doc: String = "", isPublic: Boolean = true): STTConfEntry[Boolean] =
//      STTConfEntry(key, defaultValue, { v =>
//        try {
//          v.toBoolean
//        } catch {
//          case _: IllegalArgumentException =>
//            throw new IllegalArgumentException(s"$key should be boolean, but was $v")
//        }
//      }, _.toString, doc, isPublic)
//
//    def stringConf(key: String, defaultValue: Option[String] = None,
//                   doc: String = "", isPublic: Boolean = true): STTConfEntry[String] =
//      STTConfEntry(key, defaultValue, v => v, v => v, doc, isPublic)
//  }
//
//}
