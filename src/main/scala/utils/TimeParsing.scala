package utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}


object TimeParsing {
  def timeLong2String(tm: Long, timeZone: TimeZone = TimeZone.getTimeZone("GMT+8.00")): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    fm.setTimeZone(timeZone)
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

  def nextDay(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val tim = fm.format(new Date(tm * 1000 + 1000 * 60 * 60 * 24))
    tim
  }

  def getDate(tm: Long, timeZone: TimeZone = TimeZone.getTimeZone("GMT+8.00")): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    fm.setTimeZone(timeZone)
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def getHour(tm: Long, timeZone: TimeZone = TimeZone.getTimeZone("GMT+8.00")): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    fm.setTimeZone(timeZone)
    val tim = fm.format(new Date(tm * 1000))
    println(tim)
    tim.split(" ").last.split(":").head.toInt
  }

  def date2Long(s: String, timeZone: TimeZone = TimeZone.getTimeZone("GMT+8.00")): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.setTimeZone(timeZone)
    format.parse(s).getTime / 1000
  }
}
