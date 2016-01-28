package org.deephacks.jobpipe;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

public class TimeRange {

  private final DateTime from;
  private final DateTime to;
  private final TimeRangeType type;
  private final int intervalsBetween;

  public TimeRange(String date) {
    if (date.contains("/")) {
      String[] interval = date.split("/");
      if (interval.length != 2) {
        throw new IllegalArgumentException("Invalid time range format " + date);
      }
      TimeRange from = new TimeRange(interval[0]);
      TimeRange to = new TimeRange(interval[1]);
      if (from.getType() != to.getType()) {
        throw new IllegalArgumentException("Interval have different from and to time range types.");
      }
      if (from.from.isAfter(to.from)) {
        throw new IllegalArgumentException("Interval 'from' is after 'to'");
      }
      this.type = from.getType();
      this.intervalsBetween = this.type.timeBetween(from.from, to.from);
      this.from = from.from();
      this.to = to.from();
    } else {
      this.from = new DateTime(date);
      this.type = TimeRangeType.parse(date);
      this.intervalsBetween = 1;
      this.to = this.type.next(from, intervalsBetween);
    }
  }

  TimeRange(DateTime dateTime, TimeRangeType type, int numIntervals) {
    this.from = dateTime;
    this.type = type;
    this.intervalsBetween = numIntervals;
    this.to = type.next(from, numIntervals);
  }

  public List<DateTime> days() {
    if (type == TimeRangeType.HOUR) {
      return new ArrayList<>();
    }
    return days(next().from);
  }

  public List<DateTime> days(DateTime exclusiveEnd) {
    ArrayList<DateTime> list = new ArrayList<>();
    DateTime now = from;
    while (now.isBefore(exclusiveEnd)) {
      list.add(now);
      now = now.plusDays(1);
    }
    return list;
  }

  public List<DateTime> hours() {
    return hours(next().from);
  }

  public List<DateTime> hours(DateTime exclusiveEnd) {
    ArrayList<DateTime> list = new ArrayList<>();
    DateTime now = from;
    while (now.isBefore(exclusiveEnd)) {
      list.add(now);
      now = now.plusHours(1);
    }
    return list;
  }

  public int intervalsBetween() {
    return intervalsBetween;
  }

  public TimeRangeType getType() {
    return type;
  }

  public DateTime from() {
    return from;
  }

  public DateTime to() {
    return to;
  }

  public TimeRange nextInterval() {
    return new TimeRange(type.next(from, 1), type, 1);
  }

  public TimeRange interval() {
    return new TimeRange(from, type, 1);
  }


  public TimeRange next() {
    return new TimeRange(type.next(from, intervalsBetween), type, intervalsBetween);
  }

  public TimeRange prev() {
    return new TimeRange(type.prev(from, intervalsBetween), type, intervalsBetween);
  }

  public String format() {
    return type.format().print(from);
  }

  @Override
  public String toString() {
    return type + ","  + type.format().print(from);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TimeRange range = (TimeRange) o;

    if (from != null ? !from.equals(range.from) : range.from != null) return false;
    return type == range.type;

  }

  @Override
  public int hashCode() {
    int result = from != null ? from.hashCode() : 0;
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
  }
}
