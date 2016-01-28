package org.deephacks.jobpipe;

import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

public enum TimeRangeType {

  /**
   * Ordinal according to increasing time length is important!
   */

  SECOND {
    DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

    public DateTime next(DateTime dateTime, int num) {
      return dateTime.plusSeconds(1 * num);
    }

    public DateTime prev(DateTime dateTime, int num) {
      return dateTime.minusSeconds(1 * num);
    }

    @Override
    public DateTimeFormatter format() {
      return FORMAT;
    }

    @Override
    public int timeBetween(DateTime from, DateTime to) {
      return Seconds.secondsBetween(from, to).getSeconds();
    }
  },

  MINUTE {
    DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm");

    public DateTime next(DateTime dateTime, int num) {
      return dateTime.plusMinutes(1 * num);
    }

    public DateTime prev(DateTime dateTime, int num) {
      return dateTime.minusMinutes(1 * num);
    }

    @Override
    public DateTimeFormatter format() {
      return FORMAT;
    }

    @Override
    public int timeBetween(DateTime from, DateTime to) {
      return Minutes.minutesBetween(from, to).getMinutes();
    }

  },

  HOUR {
    DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH");

    public DateTime next(DateTime dateTime, int num) {
      return dateTime.plusHours(1 * num);
    }

    public DateTime prev(DateTime dateTime, int num) {
      return dateTime.minusHours(1 * num);
    }

    @Override
    public DateTimeFormatter format() {
      return FORMAT;
    }

    @Override
    public int timeBetween(DateTime from, DateTime to) {
      return Hours.hoursBetween(from, to).getHours();
    }

  },

  DAY {
    DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");

    public DateTime next(DateTime dateTime, int num) {
      return dateTime.plusDays(1 * num);
    }

    public DateTime prev(DateTime dateTime, int num) {
      return dateTime.minusDays(1 * num);
    }

    @Override
    public DateTimeFormatter format() {
      return FORMAT;
    }

    @Override
    public int timeBetween(DateTime from, DateTime to) {
      return Days.daysBetween(from, to).getDays();
    }

  },

  WEEK {
    DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-'W'ww");

    public DateTime next(DateTime dateTime, int num) {
      return dateTime.plusWeeks(1 * num);
    }

    public DateTime prev(DateTime dateTime, int num) {
      return dateTime.minusWeeks(1 * num);
    }

    @Override
    public DateTimeFormatter format() {
      return FORMAT;
    }

    @Override
    public int timeBetween(DateTime from, DateTime to) {
      return Weeks.weeksBetween(from, to).getWeeks();
    }

  },

  MONTH {
    DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-MM");

    public DateTime next(DateTime dateTime, int num) {
      return dateTime.plusMonths(1 * num);
    }

    public DateTime prev(DateTime dateTime, int num) {
      return dateTime.minusMonths(1 * num);
    }

    @Override
    public DateTimeFormatter format() {
      return FORMAT;
    }

    @Override
    public int timeBetween(DateTime from, DateTime to) {
      return Months.monthsBetween(from, to).getMonths();
    }

  };

  public abstract DateTime next(DateTime dateTime, int num);

  public abstract DateTime prev(DateTime dateTime, int num);

  public abstract DateTimeFormatter format();

  public abstract int timeBetween(DateTime from, DateTime to);

  public static TimeRangeType parse(String date) {
    if (date == null || date.length() == 0) {
      return null;
    }
    if (canParse(SECOND.format(), date)) {
      return SECOND;
    } else if (canParse(MINUTE.format(), date)) {
      return MINUTE;
    } else if (canParse(HOUR.format(), date)) {
      return HOUR;
    } else if (canParse(DAY.format(), date)) {
      return DAY;
    } else if (canParse(MONTH.format(), date)) {
      return MONTH;
    } else if (canParse(WEEK.format(), date)) {
      return WEEK;
    }
    throw new IllegalArgumentException("Could not parse time " + date);
  }

  private static boolean canParse(DateTimeFormatter format, String date) {
    try {
      format.parseDateTime(date.toUpperCase());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public List<TimeRange> ranges(TimeRange range) {
    if (ordinal() > range.getType().ordinal()) {
      // Target job time range is less than task time range
      return new ArrayList<>();
    }
    DateTime from = range.from();
    ArrayList<TimeRange> list = new ArrayList<>();

    while (from.isBefore(range.to())) {
      list.add(new TimeRange(from, this, range.intervalsBetween()));
      from = this.next(from, range.intervalsBetween());
    }
    return list;
  }

}
