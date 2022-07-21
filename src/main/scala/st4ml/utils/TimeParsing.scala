package st4ml.utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}


object TimeParsing {
  /**
   * convert 10-bit timestamp to a string with a specified time zone
   */
  def timeLong2String(tm: Long, timeZone: TimeZone = TimeZone.getDefault): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    fm.setTimeZone(timeZone)
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def parseTemporalRange(s: String, pattern: String = "yyyy-MM-dd HH:mm:ss", timeZone: TimeZone = TimeZone.getDefault): (Long, Long) = {
    val fm = new SimpleDateFormat(pattern)
    fm.setTimeZone(timeZone)
    val tRange = if (s.split(",").head forall Character.isDigit) {
      s.split(",").map(_.toLong)
    } else {
      s.split(",").map(fm.parse(_).getTime / 1000)
    }
    (tRange.head, tRange.last)
  }

  def nextDay(tm: Long, timeZone: TimeZone = TimeZone.getDefault): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    fm.setTimeZone(timeZone)
    val tim = fm.format(new Date(tm * 1000 + 1000 * 60 * 60 * 24))
    tim
  }

  def getDate(tm: Long, timeZone: TimeZone = TimeZone.getDefault): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    fm.setTimeZone(timeZone)
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def getHour(tm: Long, timeZone: TimeZone = TimeZone.getDefault): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    fm.setTimeZone(timeZone)
    val tim = fm.format(new Date(tm * 1000))
    tim.split(" ").last.split(":").head.toInt
  }

  def date2Long(s: String, timeZone: TimeZone = TimeZone.getDefault): Long = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    fm.setTimeZone(timeZone)
    fm.parse(s).getTime / 1000
  }

  def time2Long(s: String, timeZone: TimeZone = TimeZone.getDefault): Long = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    fm.setTimeZone(timeZone)
    fm.parse(s).getTime / 1000
  }
}
