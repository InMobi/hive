package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;

import org.apache.hadoop.hive.ql.cube.parse.DateUtil;


public enum UpdatePeriod implements Named {
  SECONDLY(Calendar.SECOND, 1000, "yyyy-MM-dd-HH-mm-ss"),
  MINUTELY(Calendar.MINUTE, 60 * SECONDLY.weight(), "yyyy-MM-dd-HH-mm"),
  HOURLY(Calendar.HOUR_OF_DAY, 60 * MINUTELY.weight(), "yyyy-MM-dd-HH"),
  DAILY(Calendar.DAY_OF_MONTH, 24 * HOURLY.weight(), "yyyy-MM-dd"),
  WEEKLY(Calendar.WEEK_OF_YEAR, 7 * DAILY.weight(), "YYYY-'W'ww"),
  MONTHLY(Calendar.MONTH, 30 * DAILY.weight(), "yyyy-MM"),
  QUARTERLY(Calendar.MONTH, 3 * MONTHLY.weight(), "yyyy-MM"),
  YEARLY(Calendar.YEAR, 12 * MONTHLY.weight(), "yyyy");

  public static final long MIN_INTERVAL = SECONDLY.weight();
  private final int calendarField;
  private final long weight;
  private final String format;

  UpdatePeriod(int calendarField, long diff, String format) {
    this.calendarField = calendarField;
    this.weight = diff;
    this.format = format;
  }

  public int calendarField() {
    return this.calendarField;
  }

  public long weight() {
    return this.weight;
  }

  public long monthWeight(Date date) {
    return DateUtil.getNumberofDaysInMonth(date) * DAILY.weight();
  }

  public String format() {
    return this.format;
  }

  @Override
  public String getName() {
    return name();
  }

  public static class UpdatePeriodComparator
      implements Comparator<UpdatePeriod> {
    @Override
    public int compare(UpdatePeriod o1, UpdatePeriod o2) {
      if (o1 == null && o2 != null) {
        return -1;
      } else if (o1 != null && o2 == null) {
        return 1;
      } else if (o1 == null && o2 == null) {
        return 0;
      } else {
        if (o1.weight > o2.weight) {
          return 1;
        } else if (o1.weight < o2.weight) {
          return -1;
        } else {
          return 0;
        }
      }
    }
  }
}
