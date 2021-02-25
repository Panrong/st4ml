package utils

import java.text.SimpleDateFormat
import java.util.Date

object TimeParsing {
  def timeLong2String(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def parseTemporalRange(s: String, pattern: String = "yyyy-MM-dd HH:mm:ss"): (Long, Long) = {
    val format = new SimpleDateFormat(pattern)
    val tRange = if (s.split(",").head forall Character.isDigit) {
      s.split(",").map(_.toLong)
    } else {
      s.split(",").map(format.parse(_).getTime / 1000)
    }
    (tRange.head, tRange.last)
  }
}
