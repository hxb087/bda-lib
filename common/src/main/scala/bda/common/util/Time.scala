package bda.common.util

import com.github.nscala_time.time.Imports._


/**
  * Operations for DateTime
  * Introduction of DateTime class in nscala-time
  * fields: year, month, day, hour, minute, second. See class
  * static fields: now, lastDay, nextDay... See object StaticDateTime
  */
object Time {
  /**
    * Unix timestamp (seconds from 1970-01-01 00:00:00) to DateTime.
    * The Datetime constructor is Datetime(millisecond), so we need to product 1000.
    *
    * @return a Datetime object
    */
  def fromTimestamp(s: Long) = new DateTime(s * 1000)

  /** timestamp to day **/
  def tid2year(s: Long): Int = fromTimestamp(s).year.get()

  /** timestamp to month **/
  def tid2month(s: Long): Int = fromTimestamp(s).month.get()

  /** timestamp to week **/
  def tid2week(s: Long): Int = fromTimestamp(s).week.get()

  /**
    * timestamp to day of week
    * int MONDAY = 1;
    * int TUESDAY = 2;
    * int WEDNESDAY = 3;
    * int THURSDAY = 4;
    * int FRIDAY = 5;
    * int SATURDAY = 6;
    * int SUNDAY = 7;
    */
  def tid2dayOfWeek(s: Long): Int = fromTimestamp(s).getDayOfWeek

  /** timestamp to day **/
  def tid2day(s: Long): Int = fromTimestamp(s).day.get()

  /** timestamp to hour **/
  def tid2hour(s: Long): Int = fromTimestamp(s).getHourOfDay

  /** timestamp to minute **/
  def tid2minute(s: Long): Int = fromTimestamp(s).getMinuteOfHour

  def hoursBetween(d1: DateTime, d2: DateTime): Int =
    hoursBetween(d1, d2)

  /**
    * Parse a format string to a DateTime object
    * Format: "dd/MM/yyyy HH:mm:ss"
    *
    */
  def parseTime(s: String, format: String): Option[DateTime] = {
    val fmt = DateTimeFormat.forPattern(format);
    try {
      Some(fmt.parseDateTime(s))
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Usage:
    * {{{
    * import bda.common.util.Time._
    * dt.format("yyyyMMdd")
    * }}}
    *
    * @param dt
    */
  implicit class MyDatetime(dt: DateTime) {
    /**
      * Transform from DateTime to String with format fmt
      */
    def format(fmt: String): String =
      dt.toString(DateTimeFormat.forPattern(fmt))

    def timestamp: Long = dt.getMillis() / 1000
  }

}
