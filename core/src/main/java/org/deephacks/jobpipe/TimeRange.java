package org.deephacks.jobpipe;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

public class TimeRange {

  private DateTime from;
  private TimeRangeType type;

  public TimeRange(String date) {
    this.from = new DateTime(date);
    this.type = TimeRangeType.parse(date);
  }

  TimeRange(DateTime dateTime, TimeRangeType type) {
    this.from = dateTime;
    this.type = type;
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

  public TimeRangeType getType() {
    return type;
  }

  public DateTime from() {
    return from;
  }

  public DateTime to() {
    return next().from;
  }

  public TimeRange next() {
    return new TimeRange(type.next(from), type);
  }

  public TimeRange prev() {
    return new TimeRange(type.prev(from), type);
  }

  public String format() {
    return type.format().format(from.toDate());
  }

  @Override
  public String toString() {
    return type + ","  + type.format().format(from.toDate());
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
